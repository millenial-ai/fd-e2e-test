import argparse
import boto3
import json

def create_pipeline_execution(args):
    pipeline_name = args.pipeline_name
    execution_display_name = args.execution_display_name
    # Create a SageMaker client
    sagemaker_client = boto3.client('sagemaker')

    # Get the pipeline definition
    try:
        response = sagemaker_client.describe_pipeline(PipelineName=pipeline_name)
        pipeline_definition = response['PipelineDefinition']
    except Exception as e:
        print(f"Failed to retrieve the pipeline definition for {pipeline_name}: {e}")
        return

    # Create a new pipeline execution
    try:
        response = sagemaker_client.start_pipeline_execution(
            PipelineName=pipeline_name,
            PipelineExecutionDisplayName=execution_display_name,
            PipelineParameters=json.load(open(args.pipeline_parameter_file)) if args.pipeline_parameter_file is not None else [],
            PipelineExecutionDescription='Test pipeline',
        )
        execution_arn = response['PipelineExecutionArn']
        print(f"Pipeline execution {execution_arn} created successfully.")
    except Exception as e:
        print(f"Failed to create a pipeline execution for {pipeline_name}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Trigger a SageMaker pipeline execution")
    parser.add_argument("--pipeline-name", required=True, help="Name of the SageMaker pipeline")
    parser.add_argument("--execution-display-name", required=True, help="Display name for the new execution")
    parser.add_argument("--pipeline-parameter-file", required=False, default=None, help="Path to pipeline execution config")

    args = parser.parse_args()

    create_pipeline_execution(args)

if __name__ == "__main__":
    main()
