"""Print out failed HTCondor/DAGMan jobs and associated summaries."""
import argparse
import concurrent.futures
import os
import re
import typing
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from dateutil.parser import parse as parse_dt


def max_line_len(lines: List[str]) -> int:
    r"""Get length of longest line (split on \n) in `lines`."""
    good = []
    for ln in lines:  # pylint: disable=C0103
        good.extend(ln.split("\n"))
    return len(max(good, key=len))


class Job:  # pylint: disable=R0902
    """Encapsulates HTCondor/DAGMan job info."""

    HELD = "held"
    NON_ZERO = "non-zero"

    def __init__(
        self, dir_path: str, cluster_id: str = "", fail_type: str = "",
    ):
        self.__error_message = ""
        self.cluster_id = cluster_id
        self.dir_path = dir_path
        self.err_filepath = ""  # type: str
        self.fail_type = fail_type
        self.job_id = ""  # type: str
        self.log_filepath = ""  # type: str

        self.start_time = None  # type: Optional[datetime]
        self.end_time = None  # type: Optional[datetime]

    @property
    def job_id(self) -> str:
        """Get job_id."""
        return self.__job_id

    @job_id.setter
    def job_id(self, value: str) -> None:
        """Set job_id."""
        self.__job_id = value
        # if we have an id, find everything else
        if value:
            self.err_filepath = os.path.join(self.dir_path, f"{value}.err")
            self.log_filepath = os.path.join(self.dir_path, f"{value}.log")
            self.start_time, self.end_time = self._get_start_end_times()

    def _search_for_keywords(
        self, keywords: List[str], one_match_per_keyword: bool = False
    ) -> Tuple[Set[str], Dict[str, str]]:
        matched_keywords = set()
        keyword_lines = {}
        try:
            with open(self.err_filepath, "r") as file:
                for i, line in enumerate(file, start=1):
                    for word in keywords:
                        if word in line:
                            matched_keywords.add(word)
                            keyword_lines[str(i)] = line.strip()
                            if one_match_per_keyword:
                                keywords.remove(word)
        except FileNotFoundError:
            pass
        return matched_keywords, keyword_lines

    @property
    def error_message(self) -> str:
        """Return error message.

        Cache result.
        """
        if not self.__error_message:
            # non-zero jobs -- search for keywords, and grab last line from *.err file
            if self.fail_type == Job.NON_ZERO:
                with open(self.err_filepath, "r") as file:
                    self.__error_message = list(file)[-1].strip()

            # held jobs -- grab 'Error' line from *.log file
            elif self.fail_type == Job.HELD:
                with open(self.log_filepath, "r") as file:
                    for line in file:
                        if "Error" in line:
                            self.__error_message = line.strip()
                            break

        return self.__error_message

    def _get_start_end_times(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        def get_datetime(line: str) -> datetime:
            raw_dt = re.findall(r".*\) (.*) Job", line)[0]
            return parse_dt(raw_dt)

        start, end = None, None
        with open(self.log_filepath, "r") as file:
            for line in file:
                if self.cluster_id in line:  # skip times from past runs
                    if (not start) and ("Job executing on host" in line):  # type: ignore[unreachable]
                        start = get_datetime(line)
                    elif (not end) and (  # type: ignore[unreachable]
                        ("Job terminated" in line) or ("Job was held" in line)
                    ):
                        end = get_datetime(line)
                    if start and end:
                        break

        return start, end

    def _get_summary_title(self, verbose: int) -> str:
        """Return a formatted title."""
        if not self.fail_type:
            grade = "successful"
        elif self.fail_type == Job.HELD:
            grade = "held"
        elif self.fail_type == Job.NON_ZERO:
            grade = "returned non-zero value"

        if verbose:
            title = f"job{self.job_id} ({self.cluster_id}) {grade}"
        else:
            title = f"job{self.job_id} {grade}"

        return title

    def _get_summary_error_message(self, verbose: int) -> str:
        """Get the error message."""
        if not self.error_message:
            return ""

        if verbose == 0:
            return f" > {self.error_message}"
        elif verbose == 1:
            return f"\n> {self.error_message}"
        else:
            return (
                f"\nlast line in {self.err_filepath.split('/')[-1]}:"
                f"\n> {self.error_message}"
            )

    def _get_summary_keywords(
        self,
        keywords: Optional[List[str]] = None,
        verbose: int = False,
        add_keyword_matches: bool = True,
    ) -> Tuple[str, str]:
        """Get the keywords that are matched and their lines as strings."""
        matched_keywords_str = ""
        keyword_lines_str = ""
        if keywords:
            matched_keywords, keyword_lines = self._search_for_keywords(
                keywords, one_match_per_keyword=not add_keyword_matches
            )
            if verbose:
                matched_keywords_str += (
                    f"\nkeywords found in {self.err_filepath.split('/')[-1]}:\n> "
                )
            else:
                matched_keywords_str += "\nfound: "
            if matched_keywords:
                matched_keywords_str += f"""{", ".join(matched_keywords)}"""
                if add_keyword_matches:
                    for linenum, line in keyword_lines.items():
                        keyword_lines_str += f"\n{linenum}: {line}"
            else:
                matched_keywords_str += "None"

        return matched_keywords_str, keyword_lines_str

    def _get_summary_time_info(self, verbose: int) -> str:
        """Get the time info."""
        times = ""
        if verbose > 1:
            start = f"Start:\t{self.start_time}"
            end = f"End:\t{self.end_time}"
            if self.start_time and self.end_time:
                wall = f"Wall:\t{self.end_time-self.start_time}"
            else:
                wall = "Unknown"
            times = f"\n{start}\n{end}\n{wall}"
        return times

    def get_summary(
        self,
        keywords: Optional[List[str]] = None,
        verbose: int = 0,
        add_keyword_matches: bool = True,
    ) -> str:
        """Return formatted summary string."""
        title = self._get_summary_title(verbose)

        err_msg = self._get_summary_error_message(verbose)

        matched_keywords_str, keyword_lines_str = self._get_summary_keywords(
            keywords, verbose, add_keyword_matches
        )

        times = self._get_summary_time_info(verbose)

        # Make Separators
        length = max_line_len([title, err_msg, matched_keywords_str, times])

        stars = ""
        if verbose:
            stars = f"\n{'*'*length}"

        dots = ""
        if verbose > 1 and err_msg:
            dots = "\n..."

        dashes = ""
        if verbose > 1 and matched_keywords_str:
            dashes = f"\n{'-'*length}"

        return f"{title}{stars}{err_msg}{dots}{matched_keywords_str}{keyword_lines_str}{dashes}{times}"


def _set_job_id(path: str, filename: str, jobs: List[Job]) -> Optional[Job]:
    with open(os.path.join(path, filename), "r") as file:
        for line in file:
            for job in jobs:
                if job.cluster_id in line:
                    # filename w/o extension, 5023.log
                    job.job_id = filename.split(".")[0]
                    return job
    return None


def _get_and_split_jobs(path: str) -> Tuple[List[Job], List[Job]]:
    """Get the failed and successful cluster jobs."""

    def get_id(line: str) -> str:
        id_ = re.findall(r"\(.+\)", line.strip())[0][1:-1]
        return typing.cast(str, id_)

    failed_jobs = []
    successful_jobs = []
    prev_line = ""
    with open(os.path.join(path, "dag.nodes.log"), "r") as file:
        for line in file:
            if "(return value" in line:
                if "(return value 0)" in line:
                    successful_jobs.append(Job(path, cluster_id=get_id(prev_line)))
                else:
                    failed_jobs.append(
                        Job(path, cluster_id=get_id(prev_line), fail_type=Job.NON_ZERO)
                    )

            elif "Job was held" in line:
                failed_jobs.append(
                    Job(path, cluster_id=get_id(line), fail_type=Job.HELD)
                )

            prev_line = line

    return failed_jobs, successful_jobs


def get_all_jobs(
    path: str, max_workers: int, only_failed_ids: bool = True
) -> Tuple[List[Job], List[Job]]:
    """Return list of successful and failed jobs."""
    failed_jobs, successful_jobs = _get_and_split_jobs(path)

    # search log files for cluster ids
    workers = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as pool:
        if only_failed_ids:
            jobs = failed_jobs
        else:
            jobs = failed_jobs + successful_jobs
        for filename in [
            fn
            for fn in os.listdir(path)
            if (".log" in fn) and ("dag.nodes.log" not in fn)
        ]:
            workers.append(pool.submit(_set_job_id, path, filename, jobs))

    # get jobs, now with job_ids
    job_sum = []
    for worker in concurrent.futures.as_completed(workers):
        job = worker.result()
        if job:
            job_sum.append(job)

    if not only_failed_ids:
        successful_jobs = [j for j in job_sum if not j.fail_type]
    failed_jobs = [j for j in job_sum if j.fail_type]

    # return
    return successful_jobs, failed_jobs


def _get_job_and_summary(
    job: Job, keywords: List[str], verbose: int, add_keyword_matches: bool
) -> Tuple[Job, str]:
    summary = job.get_summary(
        keywords=keywords, verbose=verbose, add_keyword_matches=add_keyword_matches
    )
    return job, summary


def _arrange_job_summaries(
    job_summaries: List[Tuple[Job, str]], sort_by_value: str, reverse: bool
) -> List[Tuple[Job, str]]:

    try:
        # pylint: disable=C0415
        from natsort import natsorted as sort_func  # type: ignore
    except ModuleNotFoundError:
        # pylint: disable=C0103
        WARN = "\033[93m"
        ENDC = "\033[0m"
        print(
            f"{WARN}>> pip install natsort to sort jobs naturally, instead of lexicographically{ENDC}\n"
        )
        sort_func = sorted

    if not sort_by_value:
        job_summaries = sort_func(
            job_summaries, key=lambda x: x[0].job_id, reverse=reverse
        )
    elif sort_by_value == "error":
        job_summaries = sort_func(
            job_summaries, key=lambda x: x[0].error_message, reverse=reverse
        )
    elif sort_by_value == "start":
        job_summaries = sort_func(
            job_summaries, key=lambda x: x[0].start, reverse=reverse
        )
    elif sort_by_value == "end":
        job_summaries = sort_func(
            job_summaries, key=lambda x: x[0].end, reverse=reverse
        )
    elif sort_by_value == "success":
        job_summaries = sort_func(
            job_summaries, key=lambda x: x[0].fail_type, reverse=reverse
        )
    elif sort_by_value == "walltime":
        job_summaries = sort_func(
            job_summaries, key=lambda x: (x[0].end - x[0].start), reverse=reverse
        )

    return job_summaries


def get_job_summaries(  # pylint: disable=R0913
    jobs: List[Job],
    max_workers: int,
    keywords: Optional[List[str]] = None,
    sort_by_value: str = "",
    reverse: bool = False,
    verbose: int = 0,
    print_keyword_matches: bool = True,
) -> List[str]:
    """Get list of each job's summary."""
    # make messages
    workers = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as pool:
        for job in jobs:
            workers.append(
                pool.submit(
                    _get_job_and_summary, job, keywords, verbose, print_keyword_matches
                )
            )

    # get messages
    job_summaries = []  # type: List[Tuple[Job, str]]
    for worker in concurrent.futures.as_completed(workers):
        job, summary = worker.result()
        job_summaries.append((job, summary))

    # arrange messages
    job_summaries = _arrange_job_summaries(job_summaries, sort_by_value, reverse)
    return [js[1] for js in job_summaries]


def stats(failed_jobs: List[Job], successful_jobs: List[Job]) -> str:
    """Get stats."""
    total = len(failed_jobs) + len(successful_jobs)

    def percentange(numerator: int, denominator: int) -> str:
        return f"{(numerator/denominator)*100:5.2f}%"

    def div(numerator: int, denominator: int) -> str:
        prec = len(str(denominator))
        return f"{numerator:{prec}.0f}/{denominator}"

    # Successful Jobs
    success = (
        f"Successful Jobs: {div(len(successful_jobs),total)} "
        f"{percentange(len(successful_jobs),total)}\n"
    )

    held = ""
    non_zero = ""
    dashes = ""
    total_failed = ""
    # Failed Jobs
    if total:
        # Held Jobs
        held_count = len([j for j in failed_jobs if j.fail_type == Job.HELD])
        held = (
            f"Held Jobs:       {div(held_count,total)} "
            f"{percentange(held_count,total)}\n"
        )

        # Non-Zero Jobs
        non_zero_count = len([j for j in failed_jobs if j.fail_type == Job.NON_ZERO])
        non_zero = (
            f"Non-Zero Jobs:   {div(non_zero_count,total)} "
            f"{percentange(non_zero_count,total)}\n"
        )

        dashes = "---\n"

        # Total Failed
        total_failed = (
            f"Total Failed:    {div(len(failed_jobs),total)} "
            f"{percentange(len(failed_jobs),total)}\n"
        )

    equals = f"{'=' * max_line_len([success,held,non_zero,total_failed])}\n"

    return f"{success}{equals}{held}{non_zero}{dashes}{total_failed}"


def main() -> None:
    """Print out failed HTCondor/DAGMan jobs and associated summaries."""
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="path to scan for files")
    parser.add_argument(
        "-s",
        "--sort",
        dest="sort",
        choices=["error", "start", "end", "success", "walltime"],
        help="sort jobs by this value (otherwise, defaults to job id)",
    )
    parser.add_argument(
        "-r",
        "--reverse",
        dest="reverse_sort",
        default=False,
        action="store_true",
        help="reverse sort",
    )
    parser.add_argument(
        "-f",
        "--failed",
        dest="failed",
        default=False,
        action="store_true",
        help="display only failed jobs",
    )
    parser.add_argument(
        "-v",
        dest="verbose",
        default=0,
        action="count",
        help="display more information",
    )
    parser.add_argument(
        "-k",
        "--keywords",
        metavar="KEYWORDS",
        nargs="*",
        help="keywords to search for in .err files",
    )
    parser.add_argument(
        "-w",
        "--workers",
        dest="workers",
        type=int,
        default=1,
        help="workers for multi-processing",
    )
    parser.add_argument(
        "--no-print-keyword-lines",
        dest="no_print_keywords_lines",
        default=False,
        action="store_true",
        help="don't print lines containing keywords (useful if there are many matches)",
    )
    args = parser.parse_args()

    successful_jobs, failed_jobs = get_all_jobs(
        args.path, args.workers, only_failed_ids=args.failed
    )
    if args.failed:
        jobs = failed_jobs
    else:
        jobs = successful_jobs + failed_jobs
    summaries = get_job_summaries(
        jobs,
        args.workers,
        keywords=args.keywords,
        sort_by_value=args.sort,
        reverse=args.reverse_sort,
        verbose=args.verbose,
        print_keyword_matches=not args.no_print_keywords_lines,
    )
    for summary in summaries:
        print(summary, end="\n\n" if args.verbose else "\n")

    # Print stats
    print("\n")
    print(stats(failed_jobs, successful_jobs))


if __name__ == "__main__":
    main()
