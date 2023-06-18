import time
from github import Github, GithubException
import csv
import datetime

# Start time of execution
start_time = time.time()

# Specify the date range for repository extraction
start_month = 12
end_month = 1
start_year = 2023
end_year = 2013

# API key
api_key = "Your API Key"

# Initialize the PyGitHub object with the API key
per_page = 100
max_page = 4
g = Github(api_key, per_page=per_page)


# Function to format month or day with leading zero if needed
def format_number(number):
    if number >= 10:
        return str(number)
    else:
        return f'0{number}'


# Collect repositories within the date range
repositories = []


# Function to process a repository
def process_repository(repo):
    return {
        "description": repo.description,
        "full_name": repo.full_name,
        "stars": repo.stargazers_count,
        "forks": repo.forks_count,
        "watchers": repo.watchers_count,
        "language": repo.language,
        "URL": repo.html_url,
        "issues": repo.open_issues_count,
        "last_push": repo.pushed_at.strftime("%Y-%m-%d"),
        "date_created": repo.created_at.strftime("%Y-%m-%d"),
        "topics": repo.topics
    }


# Iterate over the years and months
for year in range(start_year, end_year - 1, -1):

    start_month = 12 if year != 2023 else 6
    end_month = 1

    for month in range(start_month, end_month - 1, -1):
        # Get the repositories created within the month and year, sorted by stars in descending order
        try:
            repos_paginated = g.search_repositories(query=f"created:{year}-{format_number(month)}", sort="stars",
                                                    order="desc")
            # Collect repository details
            repos = []
            for page in range(0, max_page):
                repos.extend(repos_paginated.get_page(page))

            processed_repos = map(process_repository, repos)

            # Save repository data to CSV
            csv_file = f"repositories_{year}-{format_number(month)}.csv"

            # Define CSV header
            header = ["description", "full_name", "stars", "forks", "watchers", "language", "URL", "issues",
                      "last_push", "date_created", "topics"]

            with open(csv_file, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=header)
                writer.writeheader()
                writer.writerows(processed_repos)

            print(f"Repository data saved to {csv_file}")

        except GithubException as e:
            if e.status == 403:
                remaining_limit = g.get_rate_limit().core.remaining
                if remaining_limit < 10:
                    reset_time = datetime.datetime.fromtimestamp(g.rate_limiting_resettime)
                    sleep_time = (reset_time - datetime.datetime.now()).total_seconds()
                    print(f"API rate limit exceeded. Waiting for {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    continue
                else:
                    raise
            else:
                raise

# End time of execution
end_time = time.time()

# Calculate the execution time
execution_time = end_time - start_time

# Print the execution time
print(f"Execution time: {execution_time} seconds")
