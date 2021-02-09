"""Print out failed HTCondor/DAGMan jobs and associated summaries."""
import argparse
import concurrent.futures
import logging
import os
import re
import typing
from datetime import datetime
from enum import auto, Enum
from typing import Dict, List, Optional, Set, Tuple

from dateutil.parser import parse as parse_dt


def max_line_len(lines: List[str]) -> int:
    r"""Get length of longest line (split on \n) in `lines`."""
    good = []
    for ln in lines:  # pylint: disable=C0103
        good.extend(ln.split("\n"))
    return len(max(good, key=len))


class JobExitStatus(Enum):
    """Exit status of Condor job."""

    SUCCESS = auto()
    NON_ZERO = auto()
    HELD = auto()
    SUCCESS_BEFORE_RESCUE = auto()


class Job:  # pylint: disable=R0902
    """Encapsulates HTCondor/DAGMan job info."""

    def __init__(
        self, dir_path: str, exit_status: JobExitStatus, cluster_id: str,
    ):
        self.__error_message = ""
        self.cluster_id = cluster_id
        self.dir_path = dir_path
        self.err_filepath = ""  # type: str
        self.exit_status = exit_status
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

    def failed(self) -> bool:
        """Return whether the job did fail (held or non-zero)."""
        return self.exit_status in [JobExitStatus.HELD, JobExitStatus.NON_ZERO]

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
            if self.exit_status == JobExitStatus.NON_ZERO:
                with open(self.err_filepath, "r") as file:
                    self.__error_message = list(file)[-1].strip()

            # held jobs -- grab 'Error' line from *.log file
            elif self.exit_status == JobExitStatus.HELD:
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
        if self.exit_status == JobExitStatus.SUCCESS:
            grade = "successful"
        elif self.exit_status == JobExitStatus.SUCCESS_BEFORE_RESCUE:
            grade = "successful (before rescue dag)"
        elif self.exit_status == JobExitStatus.HELD:
            grade = "held"
        elif self.exit_status == JobExitStatus.NON_ZERO:
            grade = "returned non-zero value"
        else:
            raise Exception(f"Unaccounted for exit status: {self.exit_status}")

        if verbose:
            title = f"job{self.job_id} ({self.cluster_id}) {grade}"
        else:
            title = f"job{self.job_id} {grade}"

        return title

    def _get_summary_error_message(self, verbose: int) -> Tuple[str, str]:
        """Get the error message."""
        if not self.error_message:
            return "", ""

        if verbose == 0:
            return "", f" > {self.error_message}"
        elif verbose == 1:
            return "", f"> {self.error_message}"
        else:
            return (
                f"last line in {self.err_filepath.split('/')[-1]}:",
                f"> {self.error_message}",
            )

    def _get_summary_keywords(
        self,
        keywords: Optional[List[str]] = None,
        verbose: int = False,
        add_keyword_matches: bool = True,
    ) -> Tuple[str, List[str]]:
        """Get the keywords that are matched and their lines as strings."""
        keywords_title = ""
        keyword_lines_list = []
        if keywords:
            matched_keywords, keyword_lines = self._search_for_keywords(
                keywords, one_match_per_keyword=not add_keyword_matches
            )
            if verbose > 1:
                keywords_title = (
                    f"keywords found in {self.err_filepath.split('/')[-1]}:"
                )
            else:
                keywords_title = "found:"

            if matched_keywords:
                keyword_lines_list.append(", ".join(matched_keywords))
                if add_keyword_matches:
                    for linenum, line in keyword_lines.items():
                        keyword_lines_list.append(f"{linenum}: {line}")
            else:
                keyword_lines_list = []

        return keywords_title, keyword_lines_list

    def _get_summary_time_info(self, verbose: int) -> Tuple[str, str, str]:
        """Get the time info."""
        if verbose < 2:
            return "", "", ""

        start = f"Start:\t{self.start_time}"
        end = f"End:\t{self.end_time}"
        if self.start_time and self.end_time:
            wall = f"Wall:\t{self.end_time-self.start_time}"
        else:
            wall = "Unknown"
        return start, end, wall

    def get_summary(
        self,
        keywords: Optional[List[str]] = None,
        verbose: int = 0,
        add_keyword_matches: bool = True,
    ) -> str:
        """Return formatted summary string."""
        title = self._get_summary_title(verbose)
        err_title, err_msg = self._get_summary_error_message(verbose)

        keywords_title, keyword_lines_list = self._get_summary_keywords(
            keywords, verbose, add_keyword_matches
        )
        if not keyword_lines_list and keywords:
            if verbose < 2:
                keywords_title += " None"
            else:
                keyword_lines_list = ["None"]

        start, end, wall_time = self._get_summary_time_info(verbose)

        # Make Separators
        length = max_line_len(
            [title, err_title, err_msg, keywords_title, start, end, wall_time]
        )
        stars = "*" * (length - len(title) - 1)
        dashes_nln = "-" * length + "\n"
        nln = "\n"  # '\n' isn't allowed in f-string expression parts

        def nln_it(string: str) -> str:
            if not string:
                return ""
            return string + "\n"

        summary = (
            f"{title} "
            f"{nln_it(stars) if verbose else ''}"
            f"{dashes_nln if verbose else ''}"
            f"{nln_it(err_title)}"
            f"{nln_it(err_msg)}"
            f"{dashes_nln if (err_msg and verbose > 1) else ''}"
            f"{nln_it(keywords_title)}"
            f"{nln_it(nln.join(keyword_lines_list))}"
            f"{dashes_nln if (keyword_lines_list and verbose > 1) else ''}"
            f"{nln_it(start)}"
            f"{nln_it(end)}"
            f"{nln_it(wall_time)}"
        )

        if verbose > 1:
            return nln_it(summary)
        return summary


def _set_job_id(path: str, filename: str, jobs: List[Job]) -> Optional[Job]:
    with open(os.path.join(path, filename), "r") as file:
        for line in file:
            for job in jobs:
                if job.cluster_id in line:
                    # filename w/o extension, 5023.log
                    job.job_id = filename.split(".")[0]
                    return job
    return None


def _get_jobs(path: str) -> List[Job]:
    """Get the failed and successful cluster jobs."""

    def get_id(line: str) -> str:
        id_ = re.findall(r"\(.+\)", line.strip())[0][1:-1]
        return typing.cast(str, id_)

    jobs = []
    prev_line = ""
    with open(os.path.join(path, "dag.nodes.log"), "r") as file:
        for line in file:
            # Job returned on its own accord
            if "(return value" in line:
                # Success
                if "(return value 0)" in line:
                    jobs.append(Job(path, JobExitStatus.SUCCESS, get_id(prev_line)))
                # Fail
                else:
                    jobs.append(Job(path, JobExitStatus.NON_ZERO, get_id(prev_line)))
            # Job was held
            elif "Job was held" in line:
                jobs.append(Job(path, JobExitStatus.HELD, get_id(line)))

            prev_line = line

    logging.info(
        f"Found {len([j for j in jobs if j.exit_status == JobExitStatus.SUCCESS])} success jobs"
    )
    logging.info(
        f"Found {len([j for j in jobs if j.exit_status == JobExitStatus.NON_ZERO])} non-zero jobs"
    )
    logging.info(
        f"Found {len([j for j in jobs if j.exit_status == JobExitStatus.HELD])} held jobs"
    )

    for rescue in [fn for fn in os.listdir(path) if "dag.rescue" in fn]:
        with open(os.path.join(path, rescue)) as file:
            for line in file:
                if "Nodes premarked DONE: " in line:
                    premarked = int(line.strip().split("Nodes premarked DONE: ")[1])
                    jobs.extend(
                        Job(
                            f"rescue-{rescue}-{i}",
                            JobExitStatus.SUCCESS_BEFORE_RESCUE,
                            "",
                        )
                        for i in range(premarked)
                    )
                    logging.debug(f"Found {premarked} rescue jobs in {rescue}")
                    break

    logging.info(
        f"Found {len([j for j in jobs if j.exit_status == JobExitStatus.SUCCESS_BEFORE_RESCUE])} success-before-rescue jobs"
    )

    return jobs


def get_all_jobs(
    path: str, max_workers: int, only_failed_ids: bool = True
) -> List[Job]:
    """Return list of successful and failed jobs."""
    job_by_cluster_id = {j.cluster_id: j for j in _get_jobs(path)}
    files = [
        fn for fn in os.listdir(path) if (".log" in fn) and ("dag.nodes.log" not in fn)
    ]
    lookup_jobs = list(job_by_cluster_id.values())
    if only_failed_ids:
        lookup_jobs = [j for j in lookup_jobs if j.failed()]

    # search every <job_id>.log files for cluster ids, so to set job ids
    file_workers: List[concurrent.futures.Future] = []  # type: ignore[type-arg]
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as pool:
        file_workers.extend(
            pool.submit(_set_job_id, path, f, lookup_jobs) for f in files
        )

    # get jobs, now with job_ids
    for worker in concurrent.futures.as_completed(file_workers):
        ret_job = worker.result()
        if not ret_job:
            continue
        job_by_cluster_id[ret_job.cluster_id] = ret_job

    logging.info(f"Found {len(job_by_cluster_id)} total jobs")
    return list(job_by_cluster_id.values())


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
            job_summaries, key=lambda x: x[0].exit_status, reverse=reverse
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
    workers: List[concurrent.futures.Future] = []  # type: ignore[type-arg]
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as pool:
        workers.extend(
            pool.submit(
                _get_job_and_summary, j, keywords, verbose, print_keyword_matches
            )
            for j in jobs
        )

    # get messages
    job_summaries = []  # type: List[Tuple[Job, str]]
    for worker in concurrent.futures.as_completed(workers):
        job, summary = worker.result()
        job_summaries.append((job, summary))

    # arrange messages
    job_summaries = _arrange_job_summaries(job_summaries, sort_by_value, reverse)
    return [js[1] for js in job_summaries]


def stats(jobs: List[Job]) -> str:
    """Get stats."""
    successful_jobs = [j for j in jobs if not j.failed()]
    failed_jobs = [j for j in jobs if j.failed()]

    def percentange(numerator_list: List[Job], denominator_list: List[Job]) -> str:
        return f"{(len(numerator_list)/len(denominator_list))*100:5.2f}%"

    def div(numerator_list: List[Job], denominator_list: List[Job]) -> str:
        prec = len(str(len(denominator_list)))
        return f"{len(numerator_list):{prec}.0f}/{len(denominator_list)}"

    # Successful Jobs
    success = (
        f"Successful Jobs: {div(successful_jobs,jobs)} "
        f"{percentange(successful_jobs,jobs)}\n"
    )

    held = ""
    non_zero = ""
    dashes = ""
    total_failed = ""
    # Failed Jobs
    if failed_jobs:
        # Held Jobs
        helds = [j for j in failed_jobs if j.exit_status == JobExitStatus.HELD]
        held = f"Held Jobs:       {div(helds,jobs)} " f"{percentange(helds,jobs)}\n"

        # Non-Zero Jobs
        non_zeros = [j for j in failed_jobs if j.exit_status == JobExitStatus.NON_ZERO]
        non_zero = (
            f"Non-Zero Jobs:   {div(non_zeros,jobs)} "
            f"{percentange(non_zeros,jobs)}\n"
        )

        dashes = "---\n"

        # Total Failed
        total_failed = (
            f"Total Failed:    {div(failed_jobs,jobs)} "
            f"{percentange(failed_jobs,jobs)}\n"
        )

    equals = f"{'=' * max_line_len([success, held, non_zero, total_failed])}\n"

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
    logging.basicConfig(level="DEBUG")

    # Get jobs
    jobs = get_all_jobs(args.path, args.workers, only_failed_ids=args.failed)

    # Summarize
    jobs_to_summarize = jobs
    if args.failed:
        jobs_to_summarize = [j for j in jobs if j.failed()]
    summaries = get_job_summaries(
        jobs_to_summarize,
        args.workers,
        keywords=args.keywords,
        sort_by_value=args.sort,
        reverse=args.reverse_sort,
        verbose=args.verbose,
        print_keyword_matches=not args.no_print_keywords_lines,
    )
    for summary in summaries:
        print(summary, end="\n" if args.verbose else "")

    # Print stats
    print("\n")
    print(stats(jobs))


if __name__ == "__main__":
    main()
