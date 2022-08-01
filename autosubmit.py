import time
import argparse
import os
import datetime
import logging
import sys
import logging
syspath = sys.path
sys.path = ['/home/jupyter/AoU_DRC_WGS_GATK-SV-Phase1/edit/fiss'] + syspath
import emmafiss.api as fiss_api
from emmafiss.errors import FireCloudServerError


PROJECT = os.environ['GOOGLE_PROJECT']
WORKSPACE = os.environ['WORKSPACE_NAME']
ws_bucket = os.environ['WORKSPACE_BUCKET']
NAMESPACE = os.environ['WORKSPACE_NAMESPACE']


READY = 0
NOT_YET = 1
ALREADY_SUBMITTED = 2


@retry(reraise=True,
       retry=retry_if_exception_type(FireCloudServerError),
       stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=4, min=10, max=60),
       after=after_log(logger, logging.DEBUG),
       before_sleep=before_sleep_log(logger, logging.INFO))
def _fapi_list_submissions(namespace, workspace):
  response = fiss_api.list_submissions(namespace, workspace)
  fiss_api._check_response_code(response, 200)
  return response


@retry(reraise=True,
       retry=retry_if_exception_type(FireCloudServerError),
       stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=4, min=10, max=60),
       after=after_log(logger, logging.DEBUG),
       before_sleep=before_sleep_log(logger, logging.INFO))
def _fapi_get_submission(namespace, workspace, submission_id):
  response = fiss_api.get_submission(namespace, workspace, w_sub['submissionId'])
  fiss_api._check_response_code(response, 200)
  return response


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


def ready_to_submit(batch, current, previous):
  logging.info(f"Checking {batch} status for previous ({previous}) and current ({current}) workflow...")
  current_submitted = get_entities_submitted_for_workflow(NAMESPACE, WORKSPACE, current)
  previous_succeeded = None
  if previous is not None:
    previous_succeeded = get_entities_submitted_for_workflow(NAMESPACE, WORKSPACE, previous, require_success=True)
  if previous is None or batch in previous_succeeded:
    if batch in current_submitted:
      logging.info(f"{batch} already submitted for {current}")
      return ALREADY_SUBMITTED
    else:
      logging.info(f"{batch} is ready to submit for {current}")
      return READY
  else:
    logging.info(f"{batch} has not yet successfully completed {previous}")
    return NOT_YET


def auto_submit(current, previous, interval, comment, output_log, retry=True, batches=None, dry_run=False):
  if batches is None:
    batches = [f"batch{i}_AoUSVPhaseI" for i in range(1, 25)]
  num_batches = len(batches)
  to_retry = []
  with open(output_log, 'a') as out:
    if retry:
      out.write("batch\ttimestamp\tworkflow\tsubmission_response\n")
    for i, batch in enumerate(batches):
      batch_status = ready_to_submit(batch, current, previous)
      if batch_status == READY:
        logging.info(f"Ready to submit {current} for {batch} (dry run = {dry_run})")
        if dry_run:
          out.write(f"{batch}\t{datetime.datetime.now()}\t{current}\tDryRun\n")
        else:
          sub_response = fiss_api.create_submission(NAMESPACE, WORKSPACE, NAMESPACE, current, batch, 'sample_set',
                                                    delete_intermediate_outputs=True, user_comment=comment)
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
    auto_submit(current, previous, interval, comment, output_log, retry=False, batches=to_retry, dry_run=dry_run)


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
  parser.add_argument("-b", "--batches", required=False, default=None,
                      help="Batches to try to submit (comma-separated)")
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
  auto_submit(args.current, args.previous, args.interval, args.note, args.output_log, dry_run=args.dry_run,
              batches=batches)


if __name__ == "__main__":
    main()
