# Gmail Automation and Indexing Tool

This application leverages the Gmail API to help you efficiently manage your inbox. It can index your emails locally, process them based on custom rules, and then perform automated actions on your behalf (e.g., applying labels, moving to differnet mailboxes).

There are two primary components:
1.  **Email Indexer:** Fetches emails from your inbox and stores them locally for processing.
2.  **Email Processor:** Reads the stored emails, applies user-defined rules, and executes actions on them within Gmail.

## Installation Instructions

### Prerequisites

* **Python 3.9 or newer:** You can download it from [python.org](https://www.python.org/downloads/). Verify your Python version using:
    ```bash
    python --version
    # or
    python3 --version
    ```

* It's highly recommended to use a virtual environment to manage project dependencies.

### Steps

1.  **Clone the Repository:**

    ```bash
    git clone https://github.com/Addarsh/Email-Integration.git
    cd Email-Integration
    ```

2.  **Create and Activate a Virtual Environment:**
    Run these commands in the project's root directory:

    * **macOS / Linux:**

        ```bash
        python3 -m venv .venv
        source .venv/bin/activate
        ```
    * **Windows:**
    
        ```cmd
        py -3.9 -m venv .venv  # Use `py -3.9` for Python 3.9, or just `python -m venv .venv` if it's your default
        .venv\Scripts\activate
        ```

3.  **Install Dependencies:**
    ```
    pip install -r requirements.txt
    ```

### Environment Variables

This application requires certain environment variables to be set for authentication and configuration. These variables contain sensitive information and **must not be committed directly into your version control** (ensure `.env` is in your `.gitignore`).

Create a `.env` file in the root directory of the project. Your `.env` file should contain the following:

```dotenv
# Path to the SQLite database file where indexed emails will be stored.
# Default: emails.db (recommended to keep as default unless you have specific needs)
EMAIL_DB_PATH=emails.db

# Path to your Google API client secrets JSON file.
# This file is obtained from contacting the owner of the script.
CREDENTIALS_PATH=<your_credentials_path>

# Path to the file where the user's Gmail API token will be stored after first authentication.
# Default: tokens.json (recommended to keep as default)
TOKEN_PATH=tokens.json
```

#### How to Obtain Credentials
To ensure the security of these credentials, the specific values for `CREDENTIALS_PATH` are not publicly available.

Please contact me at my email to obtain these necessary values. We will provide you with the correct configurations to get the application running.

## 1. Email indexer

This script will fetch emails from your Gmail inbox and store them locally.

Run the script from the root directory.

```bash
python -m scripts.index_emails [OPTIONS]
```

### Optional Arguments
* `--email_senders <EMAIL1> <EMAIL2> ...`: Filter emails to index only from specified senders (space-separated list).
* `--max_count <NUMBER>`: Set the maximum total number of emails to index.
* `--batch_size <NUMBER>`: Set the maximum number of emails to fetch in a single API batch request.
* `--log_level <LEVEL>`: Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL).

### Examples
```bash
# Index up to 100 emails from specific senders with INFO logging
python -m scripts.index_emails --email_senders example@domain.com another@domain.com --max_count 100 --log_level INFO

# Index all available emails (default batch size)
python -m scripts.index_emails
```

## 2. Email Processor

This script processes the previously indexed emails using defined rules and takes actions on your behalf within Gmail.

```bash
python -m scripts.process_emails [OPTIONS]
```
### Rule Configuration
By default, rules are loaded from `rules_config.json` in the project root. You can specify a different rules file using the `--rules_path` argument. Ensure your rules file is a valid JSON that defines your email processing logic (e.g., criteria for matching emails and the actions to perform).

### Optional Arguments
* `--rules_path <PATH_TO_FILE>`: Specify the path to your custom rules JSON file.
* `--log_level <LEVEL>`: Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL).

### Examples
```bash
# Process emails using the default rules file
python -m scripts.process_emails

# Process emails using a custom rules file with detailed logging
python -m scripts.process_emails --rules_path my_custom_rules.json --log_level DEBUG
```

## Unit Testing

Run unit tests from the root directory.
```
python -m pytest
```