# GitHub Actions Workflows

This project uses GitHub Actions to automate testing and ensure the QuiverQuant API client works correctly.

## Daily API Integration Tests

The workflow runs automatically in the following scenarios:
- On a daily schedule (1:00 AM UTC)
- When code is pushed to main/master branches
- When a pull request is created against main/master branches
- Manually via the GitHub Actions UI (workflow_dispatch)

### Configuration

The workflow is defined in `.github/workflows/daily-tests.yml` and:
- Runs on multiple Python versions (3.8, 3.9, 3.10)
- Installs all dependencies from requirements.txt
- Runs the full test suite against the actual QuiverQuant API
- Generates and uploads test coverage reports

### Secret Configuration

To run the tests properly, you need to configure a repository secret:

1. Go to your GitHub repository
2. Navigate to Settings > Secrets and variables > Actions
3. Create a new repository secret with:
   - Name: `QUIVERQUANT_API_TOKEN`
   - Value: Your QuiverQuant API token

Without this secret, tests requiring API authentication will be skipped.

## Local Development and Testing

To set up this project for local development:

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install development dependencies:
   ```bash
   pip install -e .
   pip install pytest pytest-cov
   ```
4. Copy the `.env.example` file to `.env` and add your API token:
   ```bash
   cp .env.example .env
   # Then edit .env to add your actual API token
   ```
5. Run the tests:
   ```bash
   python -m pytest Tests/test_quiverquant.py -v
   ```

## Modifying the Workflow

If you need to change the testing schedule or configuration:
1. Edit the `.github/workflows/daily-tests.yml` file
2. For the schedule, modify the cron expression - see [GitHub Actions documentation](https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions#schedule) for details on cron syntax
3. Commit and push your changes
