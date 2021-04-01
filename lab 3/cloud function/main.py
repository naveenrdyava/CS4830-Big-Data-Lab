
def lab3_cloudfncn(data, context):
    import apache_beam as beam
    from apache_beam.io import ReadFromText
    from apache_beam.io import WriteToText
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.options.pipeline_options import GoogleCloudOptions
    from apache_beam.options.pipeline_options import StandardOptions
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'lab-3-305308' # Enter your project ID
    google_cloud_options.job_name = 'lab-3-cloud-function'
    google_cloud_options.temp_location = "gs://naveenrd/temp_folder2" # This is to store
    google_cloud_options.region = "us-central1"
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    with  beam.Pipeline(options=options) as p:
        lines = p | 'Read' >> beam.io.ReadFromText('gs://'+data['bucket']+'/'+data['name'])  # reads lines form the text files & creates PCollection.

        # The below line of code is to count the number of lines in the text file.
        n_lines = lines | 'Count lines' >>  beam.combiners.Count.Globally() 
                        | 'Write_n_lines' >> beam.io.WriteToText('gs://naveenrd/outputs_cloud_function/n_lines.txt')