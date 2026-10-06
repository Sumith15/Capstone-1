# Validation

The Spark runtime lives in the user's WSL environment. Run the pipeline against the mounted sample there with `spark-submit src/pipeline.py --stage all`. Phase 2 validation checks should be added as Spark integration tests once the final WSL Spark version and local test runner are confirmed.
