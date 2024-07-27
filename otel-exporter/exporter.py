import json
import logging
import os
from custom_parsers import (do_parse,do_time,grab_span_att_vars, parse_attributes)
from opentelemetry import trace
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.trace import Status, StatusCode
from otel import create_resource_attributes, get_logger, get_tracer
from global_variables import *
from get_resources import get_updated_pipelines
import re
 
def send_to_otelcol(project, pipeline):    
    GLAB_SERVICE_NAME = str((project.attributes.get('name_with_namespace'))).lower().replace(" ", "")
    try:
        jobs = pipeline.jobs.list(get_all=True)
        job_lst=[]
        #Ensure we don't export data for otel exporters
        for job in jobs:
            job_json = json.loads(job.to_json())
            if str(job_json['stage']).lower() not in ["otel-exporter", "otel-metrics-exporter"]:
                job_lst.append(job_json)
                
        if len(job_lst) == 0:
            print("No data to export, assuming this pipeline jobs are otel exporters")
            return 
            
    except Exception as e:
        print(e)
        
    #Set variables to use for OTEL metrics and logs exporters
    global_resource = Resource(attributes={
    SERVICE_NAME: GLAB_SERVICE_NAME,
    "instrumentation.name": "gitlab-integration",
    "pipeline_id": str(pipeline.id),
    "project_id": str(project.id),
    "gitlab.source": "gitlab-exporter",
    "gitlab.resource.type": "span"
    })
    
    LoggingInstrumentor().instrument(set_logging_format=True,log_level=logging.INFO)
    
    #Create global tracer to export traces to Otel Collector
    tracer = get_tracer(endpoint, headers, global_resource, "tracer")
    
    #Configure env variables as span attributes
    GLAB_LOW_DATA_MODE=False
    # Check if we should run on low_data_mode
    if "GLAB_LOW_DATA_MODE" in os.environ and os.getenv('GLAB_LOW_DATA_MODE').lower() == "true":
        GLAB_LOW_DATA_MODE = True           
      
    if GLAB_LOW_DATA_MODE:
        atts = {}
    else:
        atts = grab_span_att_vars()
    
    #Configure spans
    pipeline_json = json.loads(pipeline.to_json())
    print("PIPELINE_JSON: " + pipeline.to_json())
    
    # Create a new root span(use start_span to manually end span with timestamp - set pipeline status)     
    if pipeline_json['status'] == "failed":
        p_parent.set_status(Status(StatusCode.ERROR,"Pipeline failed, check jobs for more details")) 
        #Check if pipeline start failed
        if pipeline_json['started_at'] == None:
            p_parent = tracer.start_span(name=GLAB_SERVICE_NAME + " - pipeline: "+str(pipeline.id) + " - FAILED", attributes=atts, start_time=do_time(str(pipeline_json['created_at'])), kind=trace.SpanKind.SERVER)
            p_parent.end(end_time=do_time(str(pipeline_json['created_at'])))
            return
        else: 
            p_parent = tracer.start_span(name=GLAB_SERVICE_NAME + " - pipeline: " + str(pipeline.id)+ " - FAILED", attributes=atts, start_time=do_time(str(pipeline_json['started_at'])), kind=trace.SpanKind.SERVER)
    else: 
        #In some cases even if there is not failure, the gitlab doesn't return a started_at data (e.g if the pipeline is in blocked state due to a manual job) in this case we export the pipline creation time
        if pipeline_json['started_at'] == None: 
            p_parent = tracer.start_span(name=GLAB_SERVICE_NAME + " - pipeline: "+ str(pipeline.id), attributes=atts, start_time=do_time(str(pipeline_json['created_at'])), kind=trace.SpanKind.SERVER)
        else: 
            p_parent = tracer.start_span(name=GLAB_SERVICE_NAME + " - pipeline: "+ str(pipeline.id), attributes=atts, start_time=do_time(str(pipeline_json['started_at'])), kind=trace.SpanKind.SERVER)            
    try:
        if GLAB_LOW_DATA_MODE:
            pass
        else:
            #Grab pipeline attributes
            pipeline_attributes = parse_attributes(pipeline_json)
            pipeline_attributes.update(atts)
            p_parent.set_attributes(pipeline_attributes)

        

        #Set the current span in context(parent)
        pcontext = trace.set_span_in_context(p_parent)
        for job in job_lst:
            #Set job level tracer and logger
            resource_attributes ={SERVICE_NAME: GLAB_SERVICE_NAME,"pipeline_id": str(pipeline.id),"project_id": str(project.id),"job_id": str(job["id"]),"instrumentation.name": "gitlab-integration","gitlab.source": "gitlab-exporter","gitlab.resource.type": "span"}
            if GLAB_LOW_DATA_MODE:
                pass
            else:
                job_attributes = parse_attributes(job)
                resource_attributes.update(create_resource_attributes(job_attributes,GLAB_SERVICE_NAME ))
            resource_log = Resource(attributes=resource_attributes)
            job_tracer = get_tracer(endpoint, headers, resource_log, "job_tracer")
            try:
             # Create a new child span for every valid job, set it as the current span in context
                if (job['status']) == "skipped":
                    child = job_tracer.start_span(name="Stage: " + str(job['stage'])+" - job: "+ str(job['name']) + "- SKIPPED",context=pcontext,kind=trace.SpanKind.CONSUMER)
                    child.end()
                elif (job['status']) == "manual":
                    child = job_tracer.start_span(name="Stage: " + str(job['stage'])+" - job: "+ str(job['name']) + "- MANUAL",context=pcontext,kind=trace.SpanKind.CONSUMER)
                    child.end()
                elif (job['status']) == "pending":
                    child = job_tracer.start_span(name="Stage: " + str(job['stage'])+" - job: "+ str(job['name']) + "- PENDING",context=pcontext,kind=trace.SpanKind.CONSUMER)
                    child.end()
                else:
                    child = job_tracer.start_span(name="Stage: " + str(job['stage'])+" - job: "+ str(job['name']), start_time=do_time(job['started_at']),context=pcontext, kind=trace.SpanKind.CONSUMER)
                    with trace.use_span(child, end_on_exit=False):
                        try:
                            ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
                            if job['status'] == "failed":
                                current_job = project.jobs.get(job['id'], lazy=True)
                                with open("job.log", "wb") as f:
                                    current_job.trace(streamed=True, action=f.write)
                                with open("job.log", "rb") as f:
                                    log_data = ""
                                    for string in f:
                                        log_data+=str(ansi_escape.sub('', str(string.decode('utf-8', 'ignore'))))
                                
                                match = log_data.split("ERROR: Job failed: ")
                                if do_parse(match):
                                    child.set_status(Status(StatusCode.ERROR,str(match[1])))
                                else:
                                    child.set_status(Status(StatusCode.ERROR,str(job['failure_reason'])))
                            if GLAB_LOW_DATA_MODE:
                                pass
                            else:
                                child.set_attributes(parse_attributes(job))

                            if GLAB_EXPORT_LOGS:
                                try:
                                    if job['status'] == "failed":
                                        pass
                                    else:
                                        with open("job.log", "wb") as f:
                                            current_job = project.jobs.get(job['id'], lazy=True)
                                            current_job.trace(streamed=True, action=f.write)
                                    with open("job.log", "rb") as f:  
                                        err = False
                                        for string in f:
                                            if string.decode('utf-8').startswith('ERROR:'):
                                                err = True
                                                
                                    with open("job.log", "rb") as f:
                                        resource_attributes_base ={SERVICE_NAME: GLAB_SERVICE_NAME,"pipeline_id": str(pipeline.id),"project_id": str(project.id),"job_id": str(job["id"]),"instrumentation.name": "gitlab-integration","gitlab.source": "gitlab-exporter","gitlab.resource.type": "span","stage.name":str(job_json['stage'])}
                                        if err:
                                            count = 1
                                            for string in f:
                                                txt = str(ansi_escape.sub(' ', str(string.decode('utf-8', 'ignore'))))
                                                # print("LOG LINE: " + txt)
                                                if string.decode('utf-8') != "\n" and len(txt) > 2:
                                                    if count == 1:
                                                        resource_attributes["message"] = txt
                                                        resource_attributes.update(resource_attributes_base)
                                                        resource_log = Resource(attributes=resource_attributes)
                                                        job_logger = get_logger(endpoint,headers,resource_log, "job_logger")
                                                        job_logger.error(txt)
                                                    else:
                                                        resource_attributes_base["message"] = txt
                                                        resource_log = Resource(attributes=resource_attributes_base)
                                                        job_logger = get_logger(endpoint,headers,resource_log, "job_logger")
                                                        job_logger.error(txt)
                                                    count += 1
                                        else: 
                                            count = 1
                                            for string in f:
                                                txt = str(ansi_escape.sub(' ', str(string.decode('utf-8', 'ignore'))))
                                                if string.decode('utf-8') != "\n" and len(txt) > 2:
                                                    if count == 1:
                                                        resource_attributes["message"] = txt
                                                        resource_log = Resource(attributes=resource_attributes)
                                                        job_logger = get_logger(endpoint,headers,resource_log, "job_logger")
                                                        job_logger.info(txt)
                                                    else:
                                                        resource_attributes_base["message"] = txt
                                                        resource_log = Resource(attributes=resource_attributes_base)
                                                        job_logger = get_logger(endpoint,headers,resource_log, "job_logger")
                                                        job_logger.info(txt)
                                                    count += 1

                                except Exception as e:
                                    print(e)
                            else:
                                print("Not configured to send logs Otel Collector, skip...")    

                        finally:
                            child.end(end_time=do_time(job['finished_at']))

                if job == (len(job_lst)-1):
                    print(job)

            except Exception as e:
                print(e)      

        
        print("All data sent to Otel Collector for pipeline: " + str(pipeline_json['id']))
        print("Terminating...")

    finally:
        #If pipeline is not finished, export the last updated date
        if pipeline_json['finished_at'] == None: 
            p_parent.end(end_time=do_time(str(pipeline_json['updated_at'])))
        else:    
            p_parent.end(end_time=do_time(str(pipeline_json['finished_at'])))
    
    gl.session.close()

def otel_export():
    project_id = os.getenv('CI_PROJECT_ID')
    project = gl.projects.get(project_id)        
    
    if SCHEDULED_EXPORT: 
        pipeline_ids = get_updated_pipelines(int(EXPORT_INTERVAL), str(project.id))
        for id in pipeline_ids: 
            pipeline = project.pipelines.get(id)
            print("Starting export of pipeline: " + str(pipeline.id))   
            send_to_otelcol(project, pipeline)
    else: 
        pipeline_id = os.getenv('CI_PARENT_PIPELINE')
        pipeline = project.pipelines.get(pipeline_id)   
        send_to_otelcol(project, pipeline)

otel_export()