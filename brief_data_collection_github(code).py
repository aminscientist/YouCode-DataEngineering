import time
from github import Github, GithubException
from tqdm import tqdm
import csv
import datetime
import concurrent.futures

# Start time of execution
start_time = time.time()

# Specify the date range for repository extraction
start_month = 12
end_month = 1
start_year = 2023
end_year = 2013

# API key
api_key = "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"

# Initialize the PyGitHub object with the API key
g = Github(api_key)

# Function to format month or day with leading zero if needed
def format_number(number):
    if number >= 10:
        return str(number)
    else:
        return f'0{number}'

# Collect repositories within the date range
repositories = []

# Cache for storing repository details
repository_cache = {}

# Function to process a repository
def process_repository(repo):

        # Retrieve the repository object using its full name
        repository = g.get_repo(repo.full_name)

        # Get languages and their line counts
        languages = repository.get_languages()
        language_line_counts = {language: repository.get_languages().get(language, 0) for language in languages}

        # Get the number of commits and pull requests
        commits_count = repository.get_commits().totalCount
        pull_requests_count = repository.get_pulls(state='all').totalCount

        data = {
            "description": repo.description,
            "stars": repo.stargazers_count,
            "forks": repo.forks_count,
            "Watchers": repo.watchers_count,
            "Contributors": repository.get_contributors().totalCount,
            "Languages": language_line_counts,
            "URL": repo.html_url,
            "Commits": commits_count,
            "Pull Requests": pull_requests_count
        }

        return data

# Iterate over the years and months
for year in range(start_year, end_year - 1, -1):
    if year == 2023:
        start_month = 6
    else:
        end_month = 1
    for month in range(start_month, end_month - 1, -1):
        # Check if repository details are available in cache
        cache_key = f"{year}-{format_number(month)}"
        if cache_key in repository_cache:
            repositories.extend(repository_cache[cache_key])
            continue

        # Get the repositories created within the month and year, sorted by stars in descending order
        try:
            created_repos = g.search_repositories(query=f"created:{year}-{format_number(month)}", sort="stars", order="desc")

            # Create a progress bar
            progress_bar = tqdm(total=200, desc=f"Repositories Extracted for {year}-{format_number(month)}")

            # Collect repository details
            repository_data = []

            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Submit repository processing tasks
                futures = [executor.submit(process_repository, repo) for repo in created_repos[:200]]

                # Iterate over completed tasks and collect data
                for future in concurrent.futures.as_completed(futures):
                    data = future.result()
                    if data:
                        repository_data.append(data)
                        progress_bar.update(1)

            progress_bar.close()

            # Update cache with repository details
            repository_cache[cache_key] = repository_data

            repositories.extend(repository_data)

            # Save repository data to CSV
            csv_file = f"repositories_{year}-{format_number(month)}.csv"

            # Define CSV header
            header = ["description", "stars", "forks", "Watchers", "Issues", "Contributors", "Date Created", "Languages", "URL", "Commits", "Pull Requests"]

            with open(csv_file, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=header)
                writer.writeheader()
                writer.writerows(repository_data)

            print(f"Repository data saved to {csv_file}")

            time.sleep(10)
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

    time.sleep(1000)


# Save all repository data to CSV
csv_file = "repositories_all.csv"

# Define CSV header
header = ["description", "stars", "forks", "Watchers", "Issues", "Contributors", "Date Created", "Languages", "URL", "Commits", "Pull Requests"]

with open(csv_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=header)
    writer.writeheader()
    writer.writerows(repositories)

# Print the count of repositories extracted
print(f"Number of repositories extracted: {len(repositories)}")
print(f"Repository data saved to {csv_file}")

# End time of execution
end_time = time.time()

# Calculate the execution time
execution_time = end_time - start_time

# Print the execution time
print(f"Execution time: {execution_time} seconds")
