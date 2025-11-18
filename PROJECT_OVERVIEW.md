# Project Overview

## Purpose

This project provides a Unity Catalog function that allows LLMs to trigger Databricks Jobs/Flows through the UC Function Toolkit. It's designed for testing GenAI integration with Databricks workflows.

## What You Get

✅ **One UC Function**: `trigger_databricks_flow` - Trigger any Databricks job with various parameter types  
✅ **Easy Registration**: Python script to create the UC function  
✅ **Test Suite**: Comprehensive test notebook with multiple examples  
✅ **Local Testing**: CLI tool to test before UC registration  
✅ **Full Documentation**: README, QuickStart guide, and examples  

## Quick Links

- **Get Started**: Read [QUICKSTART.md](QUICKSTART.md) for 5-minute setup
- **Full Docs**: See [README.md](README.md) for detailed documentation
- **Test It**: Use [test_function.py](test_function.py) in Databricks
- **Local Test**: Run `python local_test.py --help` for CLI usage

## File Structure

```
.
├── trigger_flow_function.py     # Core Python function
├── register_uc_functions.py     # UC registration script
├── test_function.py             # Databricks test notebook
├── local_test.py                # Local CLI testing tool
├── example_usage.py             # Additional examples
├── requirements.txt             # Dependencies
├── QUICKSTART.md                # 5-minute guide
├── README.md                    # Full documentation
└── PROJECT_OVERVIEW.md          # This file
```

## Workflow

1. **Install**: `pip install -r requirements.txt`
2. **Register**: Run `register_uc_functions.py` in Databricks
3. **Test**: Use `test_function.py` to validate
4. **Use**: Call from SQL, Python, or GenAI agents

## Key Features

### Comprehensive Parameter Support
- Notebook parameters (key-value pairs)
- Python command-line arguments
- JAR parameters
- Delta Live Tables pipeline parameters
- SQL query parameters
- dbt commands

### Error Handling
Returns structured JSON with success status and error details.

### Idempotency
Supports idempotency tokens to prevent duplicate job runs.

### LLM-Friendly
Designed specifically for UC Function Toolkit with clear docstrings.

## Use Cases

1. **GenAI Workflow Automation**: Enable LLMs to trigger data pipelines
2. **Event-Driven Processing**: Start jobs based on AI agent decisions
3. **Interactive Data Operations**: Let users trigger jobs via chat interfaces
4. **Testing**: Validate job configurations programmatically

## Authentication

Uses Databricks SDK default authentication:
- Databricks CLI config
- Environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN)
- Azure/GCP authentication
- Automatic in Databricks notebooks

## Next Steps After Setup

1. **Grant Permissions**: Give users/agents EXECUTE rights
2. **Integrate with Agent**: Add to UC Function Toolkit
3. **Monitor**: Set up logging for job triggers
4. **Extend**: Add more functions (status check, listing, etc.)

## Support

- Check [README.md](README.md) for troubleshooting
- Review [test_function.py](test_function.py) for examples
- Test locally with `local_test.py` before UC registration

## License

This is a sample project for testing Databricks GenAI capabilities.
