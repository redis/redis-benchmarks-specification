import logging
from github import Github


def generate_build_finished_pr_comment(
    benchmark_stream_ids,
    commit_datetime,
    commit_summary,
    git_branch,
    git_hash,
    tests_groups_regexp,
    tests_priority_lower_limit,
    tests_priority_upper_limit,
    tests_regexp,
):
    comment_body = "### CE Performance Automation : step 1 of 2 (build) done\n\n"
    comment_body += (
        "This comment was automatically generated given a benchmark was triggered.\n"
    )
    comment_body += "You can check each build/benchmark progress in grafana:\n"
    comment_body += f"   - git hash: {git_hash}\n"
    comment_body += f"   - git branch: {git_branch}\n"
    comment_body += f"   - commit date and time: {commit_datetime}\n"
    comment_body += f"   - commit summary: {commit_summary}\n"
    comment_body += f"   - test filters:\n"
    comment_body += (
        f"       - command priority lower limit: {tests_priority_lower_limit}\n"
    )
    comment_body += (
        f"       - command priority upper limit: {tests_priority_upper_limit}\n"
    )
    comment_body += f"       - test name regex: {tests_regexp}\n"
    comment_body += f"       - command group regex: {tests_groups_regexp}\n\n"
    for benchmark_stream_id in benchmark_stream_ids:
        benchmark_stream_id = benchmark_stream_id.decode()
        grafana_benchmark_status_link = f"https://benchmarksredisio.grafana.net/d/edsxdsrbexhc0f/ce-benchmark-run-status?orgId=1&var-benchmark_work_stream={benchmark_stream_id}"
        print("=============================================================")
        print(f"Check benchmark run status in: {grafana_benchmark_status_link}")
        comment_body += f"You can check a comparison in detail via the [grafana link]({grafana_benchmark_status_link})"
    return comment_body


def check_github_available_and_actionable(
    fn, github_token, pull_request, tf_github_org, tf_github_repo, verbose
):
    # using an access token
    is_actionable_pr = False
    contains_regression_comment = False
    regression_comment = None
    github_pr = None
    old_regression_comment_body = ""
    if github_token is not None:
        logging.info("Detected github token")
        g = Github(github_token)
        if pull_request is not None and pull_request != "":
            pull_request_n = int(pull_request)
            github_pr = (
                g.get_user(tf_github_org)
                .get_repo(tf_github_repo)
                .get_issue(pull_request_n)
            )
            comments = github_pr.get_comments()
            pr_link = github_pr.html_url
            logging.info("Working on github PR already: {}".format(pr_link))
            is_actionable_pr = True
            contains_regression_comment, pos = fn(comments)
            if contains_regression_comment:
                regression_comment = comments[pos]
                old_regression_comment_body = regression_comment.body
                logging.info(
                    "Already contains PR comment. Link: {}".format(
                        regression_comment.html_url
                    )
                )
                if verbose:
                    logging.info("Printing old PR comment:")
                    print("".join(["-" for x in range(1, 80)]))
                    print(regression_comment.body)
                    print("".join(["-" for x in range(1, 80)]))
            else:
                logging.info("Does not contain PR comment")
    return (
        contains_regression_comment,
        github_pr,
        is_actionable_pr,
        old_regression_comment_body,
        pr_link,
        regression_comment,
    )


def create_new_pr_comment(auto_approve, comment_body, github_pr, pr_link):
    regression_comment = None
    user_input = "n"
    if auto_approve:
        print("auto approving...")
    else:
        user_input = input("Do you wish to add a comment in {} (y/n): ".format(pr_link))
    if user_input.lower() == "y" or auto_approve:
        print("creating an comment in PR {}".format(pr_link))
        regression_comment = github_pr.create_comment(comment_body)
        html_url = regression_comment.html_url
        print("created comment. Access it via {}".format(html_url))
    return regression_comment


def update_comment_if_needed(
    auto_approve, comment_body, old_regression_comment_body, regression_comment, verbose
):
    same_comment = False
    user_input = "n"
    if comment_body == old_regression_comment_body:
        logging.info(
            "The old regression comment is the same as the new comment. skipping..."
        )
        same_comment = True
    else:
        logging.info(
            "The old regression comment is different from the new comment. updating it..."
        )
        comment_body_arr = comment_body.split("\n")
        old_regression_comment_body_arr = old_regression_comment_body.split("\n")
        if verbose:
            DF = [
                x for x in comment_body_arr if x not in old_regression_comment_body_arr
            ]
            print("---------------------")
            print(DF)
            print("---------------------")
    if same_comment is False:
        if auto_approve:
            print("auto approving...")
        else:
            user_input = input(
                "Do you wish to update the comment {} (y/n): ".format(
                    regression_comment.html_url
                )
            )
        if user_input.lower() == "y" or auto_approve:
            print("Updating comment {}".format(regression_comment.html_url))
            regression_comment.edit(comment_body)
            html_url = regression_comment.html_url
            print(
                "Updated comment. Access it via {}".format(regression_comment.html_url)
            )


def check_benchmark_build_comment(comments):
    res = False
    pos = -1
    for n, comment in enumerate(comments):
        body = comment.body
        if "CE Performance Automation : step" in body:
            res = True
            pos = n
    return res, pos


def check_benchmark_running_comment(comments):
    res = False
    pos = -1
    for n, comment in enumerate(comments):
        body = comment.body
        if "CE Performance Automation : step" in body:
            res = True
            pos = n
    return res, pos


def markdown_progress_bar(current, total, bar_length=40):
    progress = current / total
    block = int(round(bar_length * progress))
    bar = "#" * block + "-" * (bar_length - block)
    percentage = round(progress * 100, 2)
    return f"[{bar}] {percentage}%"


def generate_benchmark_started_pr_comment(
    benchmark_stream_ids,
    total_pending,
    total_benchmarks,
):
    comment_body = (
        "### CE Performance Automation : step 2 of 2 (benchmark) RUNNING...\n\n"
    )
    comment_body += (
        "This comment was automatically generated given a benchmark was triggered.\n\n"
    )

    completed = total_benchmarks - total_pending
    comment_body += (
        f"Status: {markdown_progress_bar(completed,total_benchmarks)} pending.\n\n"
    )
    comment_body += f"In total will run {total_benchmarks} benchmarks.\n"
    comment_body += f"    - {total_pending} pending.\n"
    comment_body += f"    - {total_pending} completed.\n"

    for benchmark_stream_id in benchmark_stream_ids:
        benchmark_stream_id = benchmark_stream_id.decode()
        grafana_benchmark_status_link = f"https://benchmarksredisio.grafana.net/d/edsxdsrbexhc0f/ce-benchmark-run-status?orgId=1&var-benchmark_work_stream={benchmark_stream_id}"
        comment_body += f"You can check a the status in detail via the [grafana link]({grafana_benchmark_status_link})"
    return comment_body
