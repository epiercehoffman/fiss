import time
import argparse
import os
import datetime
import logging
from tenacity import retry, after_log, before_sleep_log, retry_if_exception_type, stop_after_attempt, wait_exponential
import requests
import firecloud.api as fiss_api
from firecloud.errors import FireCloudServerError


logger = logging.getLogger()
logger.setLevel(logging.INFO)


PROJECT = os.environ['GOOGLE_PROJECT']
WORKSPACE = os.environ['WORKSPACE_NAME']
ws_bucket = os.environ['WORKSPACE_BUCKET']
NAMESPACE = os.environ['WORKSPACE_NAMESPACE']


READY = 0
NOT_YET = 1
ALREADY_SUBMITTED = 2


@retry(reraise=True,
       retry=retry_if_exception_type((FireCloudServerError, requests.ConnectionError)),
       stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=4, min=10, max=60),
       after=after_log(logger, logging.DEBUG),
       before_sleep=before_sleep_log(logger, logging.INFO))
def _fapi_list_submissions(namespace, workspace):
  response = fiss_api.list_submissions(namespace, workspace)
  fiss_api._check_response_code(response, 200)
  return response


@retry(reraise=True,
       retry=retry_if_exception_type((FireCloudServerError, requests.ConnectionError)),
       stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=4, min=10, max=60),
       after=after_log(logger, logging.DEBUG),
       before_sleep=before_sleep_log(logger, logging.INFO))
def _fapi_get_submission(namespace, workspace, submission_id):
  response = fiss_api.get_submission(namespace, workspace, submission_id)
  fiss_api._check_response_code(response, 200)
  return response


@retry(reraise=True,
       retry=retry_if_exception_type((FireCloudServerError, requests.ConnectionError)),
       stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=4, min=10, max=60),
       after=after_log(logger, logging.DEBUG),
       before_sleep=before_sleep_log(logger, logging.INFO))
def _fapi_get_method_config(namespace, workspace, cnamespace, config):
  response = fiss_api.get_workspace_config(namespace, workspace, cnamespace, config)
  fiss_api._check_response_code(response, 200)
  return response


@retry(reraise=True,
       retry=retry_if_exception_type((FireCloudServerError, requests.ConnectionError)),
       stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=4, min=10, max=60),
       after=after_log(logger, logging.DEBUG),
       before_sleep=before_sleep_log(logger, logging.INFO))
def _fapi_get_entity(namespace, workspace, entity_type, entity_name):
  response = fiss_api.get_entity(namespace, workspace, entity_type, entity_name)
  fiss_api._check_response_code(response, 200)
  return response


def get_members_in_set(entity_type, entity_name):
  if not entity_type.endswith("_set"):
    raise ValueError(f"Members cannot be found for entity type {entity_type} because it is not a set")
  member_type = entity_type[:-4] + "s"
  result = _fapi_get_entity(NAMESPACE, WORKSPACE, entity_type, entity_name).json()
  members = [item['entityName'] for item in result['attributes'][member_type]['items']]
  return set(members)


def get_entities_submitted_for_workflow(namespace, workspace, workflow, require_success=False):
  response = _fapi_list_submissions(NAMESPACE, WORKSPACE)
  result = response.json()
  workflow_submissions = [sub for sub in result if sub['methodConfigurationName'] == workflow]
  entities_submitted = set()
  for w_sub in workflow_submissions:
    detailed = None
    if require_success or sum(w_sub['workflowStatuses'].values()) > 1:
      detailed_response = _fapi_get_submission(namespace, workspace, w_sub['submissionId'])
      detailed = detailed_response.json()
    if sum(w_sub['workflowStatuses'].values()) > 1:
      for w in detailed['workflows']:
          if require_success and w['status'] != "Succeeded":
            continue
          entities_submitted.add(w['workflowEntity']['entityName'])
    else:
      if require_success and detailed['workflows'][0]['status'] != "Succeeded":
        continue
      entities_submitted.add(w_sub['submissionEntity']['entityName'])
  return entities_submitted


def check_entity_status_for_workflow(namespace, workspace, workflow, entity_name, entity_type, method_namespace,
                                      require_success=False):
  logging.info(f"Checking status of {workflow} for {entity_name}")
  workflow_root_entity_type = _fapi_get_method_config(NAMESPACE, WORKSPACE, method_namespace, workflow).json()['rootEntityType']
  if workflow_root_entity_type not in entity_type:
    raise ValueError(f"{workflow} workflow root entity type is not a match or subset of submission entity type {entity_type}. Unable to check status of {entity_name}.")
  entity_name_members = None
  members_submitted = set()
  response = _fapi_list_submissions(NAMESPACE, WORKSPACE)
  result = response.json()
  workflow_submissions = [sub for sub in result if sub['methodConfigurationName'] == workflow]
  if len(workflow_submissions) == 0:
    logging.info(f"No submissions found for workflow {workflow}")
  submissions_check_members = []
  for w_sub in workflow_submissions:
    w_sub_statuses = w_sub['workflowStatuses']
    submission_entity_type = w_sub['submissionEntity']['entityType']
    # set to set, sample to sample
    # if submission was for entity_name, check status and return if success (or if require_success == False)
    if w_sub['submissionEntity']['entityName'] == entity_name:
      if require_success and not (len(w_sub_statuses) == 1 and "Succeeded" in w_sub_statuses.keys()):
        continue
      return True
    # set_set to set, set to sample
    # if submission was batched and submission type is a set of entity_type, check workflows within for entity_name
    elif entity_type + "_set" == submission_entity_type:
      detailed_response = _fapi_get_submission(namespace, workspace, w_sub['submissionId'])
      detailed = detailed_response.json()
      for w in detailed['workflows']:
        if require_success and w['status'] != "Succeeded":
          continue
        if w['workflowEntity']['entityName'] == entity_name:
          return True
    # sample to set
    # if entity_type is a set of submission type & workflow root entity type, check status for members of entity_name
    elif workflow_root_entity_type + "_set" == entity_type and workflow_root_entity_type == submission_entity_type:
      if entity_name_members is None:
        entity_name_members = get_members_in_set(entity_type, entity_name)
      if w_sub['submissionEntity']['entityName'] in entity_name_members:
        if require_success and not (len(w_sub_statuses) == 1 and "Succeeded" in w_sub_statuses.keys()):
          continue
        members_submitted.add(w_sub['submissionEntity']['entityName'])
    # setA (+ setB + sample) to set
    # if entity_type is a set of submission type, check status for members of entity_name
    elif workflow_root_entity_type + "_set" == entity_type and workflow_root_entity_type + "_set" == submission_entity_type:
      submissions_check_members.append(w_sub)  # check after the rest of the submissions - more efficient if member check not needed

  for w_sub in submissions_check_members:
    if entity_name_members is None:
      entity_name_members = get_members_in_set(entity_type, entity_name)
    detailed_response = _fapi_get_submission(namespace, workspace, w_sub['submissionId'])
    detailed = detailed_response.json()
    for w in detailed['workflows']:
      if w['workflowEntity']['entityName'] in entity_name_members:
        if require_success and w['status'] != "Succeeded":
          continue
        members_submitted.add(w['workflowEntity']['entityName'])

  if entity_name_members is not None and len(members_submitted) == len(entity_name_members):
    return True
  return False


def ready_to_submit(batch, current, previous, entity_type, current_method_namespace, previous_method_namespace):
  logging.info(f"Checking {batch} status for previous ({previous}) and current ({current}) workflow...")
  current_submitted = check_entity_status_for_workflow(NAMESPACE, WORKSPACE, current, batch, entity_type,
                                                        current_method_namespace)
  previous_succeeded = True
  if current_submitted:
    logging.info(f"{batch} already submitted for {current}")
    return ALREADY_SUBMITTED
  else:
    if previous is not None:
      previous_succeeded = check_entity_status_for_workflow(NAMESPACE, WORKSPACE, previous, batch,
                                                            entity_type, previous_method_namespace,
                                                            require_success=True)
    if previous_succeeded:
      logging.info(f"{batch} is ready to submit for {current}")
      return READY
    else:
      logging.info(f"{batch} has not yet successfully completed {previous}")
      return NOT_YET


def auto_submit(current, previous, interval, comment, output_log, retry=True,
                batches=None, dry_run=False, submission_entity_type='sample_set', expression=None,
                memory_retry_multiplier=None, call_cache=True, current_method_namespace=NAMESPACE,
                previous_method_namespace=NAMESPACE, unsafe=False):
  if unsafe:
    logging.info("Unsafe mode enabled, will not check for previous submissions before submitting.")
  num_batches = len(batches)
  to_retry = []
  current_root_entity_type = _fapi_get_method_config(NAMESPACE, WORKSPACE, current_method_namespace, current).json()['rootEntityType']
  if current_root_entity_type != submission_entity_type and expression is None:
    raise ValueError("Submission entity does not match workflow root entity type and no expression is provided")
  with open(output_log, 'a') as out:
    if retry:
      out.write("batch\ttimestamp\tworkflow\tsubmission_response\n")
    for i, batch in enumerate(batches):
      batch_status = READY
      if not unsafe:
        batch_status = ready_to_submit(batch, current, previous, submission_entity_type, current_method_namespace,
                                      previous_method_namespace)
      if batch_status == READY:
        logging.info(f"Ready to submit {current} for {batch} (dry run = {dry_run})")
        if dry_run:
          out.write(f"{batch}\t{datetime.datetime.now()}\t{current}\tDryRun\n")
        else:
          sub_response = fiss_api.create_submission(NAMESPACE, WORKSPACE, current_method_namespace, current,
                                                    entity=batch, etype=submission_entity_type, expression=expression,
                                                    delete_intermediate_output_files=True, user_comment=comment,
                                                    memory_retry_multiplier=memory_retry_multiplier, use_callcache=call_cache,
                                                    workflow_failure_mode="ContinueWhilePossible")
          status_text = "succeeded"
          waiting_text = ""
          response_log_text = "Success"
          wait = False
          if sub_response.ok:
            if i < (num_batches - 1):
              waiting_text = f" Waiting {interval} minutes for next submission."
              wait = True
          else:
            status_text = "failed"
            response_log_text = sub_response.text
            to_retry.append(batch)
          logging.info(f"Submission of {current} for {batch} {status_text}." + waiting_text + "..")
          out.write(f"{batch}\t{datetime.datetime.now()}\t{current}\t{response_log_text}\n")
          if wait:
            time.sleep(interval * 60)
      elif batch_status == NOT_YET:
        to_retry.append(batch)
  if retry and len(to_retry) > 0:
    auto_submit(current, previous, interval, comment, output_log, retry=False, batches=to_retry, dry_run=dry_run,
                submission_entity_type=submission_entity_type, expression=expression,
                memory_retry_multiplier=memory_retry_multiplier, call_cache=call_cache,
                current_method_namespace=current_method_namespace, previous_method_namespace=previous_method_namespace)


def main():
  parser = argparse.ArgumentParser()

  parser.add_argument("-o", "--output-log", required=True, help="Output log file")
  parser.add_argument("-c", "--current", required=True,
                      help="Current workflow to submit")
  parser.add_argument("-p", "--previous", required=False, default=None,
                      help="Previous workflow, check for success before submitting current")
  parser.add_argument("-i", "--interval", required=True, type=int,
                      help="Submission interval, in minutes (int)")
  parser.add_argument("-n", "--note", required=False, default=None,
                      help="Submission comment text")
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument("-b", "--batches", default=None,
                      help="Batches (or entities) to try to submit (comma-separated)")
  group.add_argument("-f", "--batches-file", default=None,
                      help="Batches (or entities) to try to submit (file, one batch name per line)")
  parser.add_argument("-s", "--submission-entity-type", required=False, default="sample_set",
                      help="Terra entity type for submission, ie. (a) 'sample_set' to submit FilterBatchSites "
                      "on one batch at a time, or (b) 'sample_set' to submit GatherSampleEvidence on a batch "
                      "of multiple samples at once. If this type does not match the root entity type of the "
                      "workflow, -e / --expression must be provided. For example in case (b), the expression "
                      "should be 'this.samples' because the root entity type is sample.")
  parser.add_argument("-e", "--expression", required=False, default=None,
                      help="If submission entity does not match root entity type of workflow, provide expression "
                      "to specify the relationship between the submission entity and the root entity. "
                      "See help text for submission entity for example.")
  parser.add_argument("-m", "--memory-retry-multiplier", default=None, required=False, type=float,
                      help="Memory retry multiplier, ie. 1.8")
  parser.add_argument("--no-call-cache", required=False, default=True, action='store_false',
                      help="Do not enable call caching (call caching enabled by default)")
  parser.add_argument("-z", "--current-method-namespace", required=False, default=None,
                      help="Namespace for current workflow, if different from workspace namespace.")
  parser.add_argument("-y", "--previous-method-namespace", required=False, default=None,
                      help="Namespace for previous workflow, if different from workspace namespace.")
  parser.add_argument("-u", "--unsafe", required=False, default=False, action='store_true',
                      help="Do not check for previous submissions of current or preceding workflow.")
  parser.add_argument("--dry-run", required=False, default=False, action='store_true',
                      help="Dry run: don't submit anything")
  parser.add_argument("-l", "--log-level", required=False, default="INFO",
                      help="Specify level of logging information, ie. info, warning, error (not case-sensitive). "
                           "Default: INFO")

  args = parser.parse_args()

  # Set logging level from -l input
  log_level = args.log_level
  numeric_level = getattr(logging, log_level.upper(), None)
  if not isinstance(numeric_level, int):
      raise ValueError('Invalid log level: %s' % log_level)
  logging.basicConfig(level=numeric_level, format='%(levelname)s: %(message)s')

  batches = None
  if args.batches is not None:
    batches = args.batches.split(',')
  elif args.batches_file is not None:
    with open(args.batches_file, 'r') as bfile:
      batches = [line.strip() for line in bfile]

  current_method_namespace = NAMESPACE
  if args.current_method_namespace is not None:
    current_method_namespace = args.current_method_namespace
  previous_method_namespace = NAMESPACE
  if args.previous_method_namespace is not None:
    previous_method_namespace = args.previous_method_namespace

  call_cache = not args.no_call_cache

  auto_submit(args.current, args.previous, args.interval, args.note, args.output_log, dry_run=args.dry_run,
              batches=batches, submission_entity_type=args.submission_entity_type, expression=args.expression,
              memory_retry_multiplier=args.memory_retry_multiplier, call_cache=call_cache, unsafe=args.unsafe,
              current_method_namespace=current_method_namespace, previous_method_namespace=previous_method_namespace)


if __name__ == "__main__":
    main()
