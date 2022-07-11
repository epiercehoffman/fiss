import time
import argparse
import os
import datetime
import logging
import sys
syspath = sys.path
sys.path = ['/home/jupyter/AoU_DRC_WGS_GATK-SV-Phase1/edit/fiss'] + syspath
import emmafiss.api as fiss_api


PROJECT = os.environ['GOOGLE_PROJECT']
WORKSPACE = os.environ['WORKSPACE_NAME']
ws_bucket = os.environ['WORKSPACE_BUCKET']
NAMESPACE = os.environ['WORKSPACE_NAMESPACE']


def get_entities_submitted_for_workflow(namespace, workspace, workflow, require_success=False):
  response = fiss_api.list_submissions(NAMESPACE, WORKSPACE)
  if not response.ok:
    logger.error(f"Failed to list submissions in {namespace}/{workspace}")
    raise FireCloudServerError(response.status_code, response.text)
  result = response.json()
  workflow_submissions = [sub for sub in result if sub['methodConfigurationName'] == workflow]
  entities_submitted = set()
  for w_sub in workflow_submissions:
    detailed = None
    if require_success or sum(w_sub['workflowStatuses'].values()) > 1:
      detailed_response = fiss_api.get_submission(namespace, workspace, w_sub['submissionId'])
      if not detailed_response.ok:
          logger.error(f"Failed to get submission {w_sub['submissionId']} in workspace {namespace}/{workspace}.")
          raise FireCloudServerError(detailed_response.status_code, detailed_response.text)
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
  current_submitted = get_entities_submitted_for_workflow(NAMESPACE, WORKSPACE, '07a-FilterBatchSites')
  previous_succeeded = get_entities_submitted_for_workflow(NAMESPACE, WORKSPACE, '06-GenerateBatchMetrics')
  ready = previous_succeeded - current_submitted
  return (batch in ready)


def auto_submit(current, previous, interval, comment, output_log, retry=True, batches=None, dry_run=False):
  if batches is None:
    batches = [f"batch{i}_AoUSVPhaseI" for i in range(1,25)]
  to_retry = []
  with open(output_log, 'a') as out:
    if retry:
      output_log.write("batch\ttimestamp\tworkflow\tsubmission_ok\tsubmission_response_text\n")
    for batch in batches:
      if ready_to_submit(batch, current, previous):
        if dry_run:
          output_log.write(f"{batch}\t{datetime.datetime.now()}\t{current}\tFalse\tDryRun\n")
        else:
          sub_response = fiss_api.create_submission(NAMESPACE, WORKSPACE, NAMESPACE, current, batch, 'sample_set',
                                                  delete_intermediate_outputs=True, user_comment=comment)
          if sub_response.ok:
            time.sleep(interval)
          else:
            to_retry.append(batch)
          output_log.write(f"{batch}\t{datetime.datetime.now()}\t{current}\t{sub_response.ok}\t{sub_response.text}\n")
      else:
        to_retry.append(batch)
  if retry and len(to_retry) > 0:
    auto_submit(current, previous, interval, comment, output_log, retry=False, batches=to_retry, dry_run=dry_run)



def main():
  parser = argparse.ArgumentParser()

  parser.add_argument("-o", "--output-log", required=True, help="Output log file")
  parser.add_argument("-c", "--current", required=True,
                      help="Current workflow to submit")
  parser.add_argument("-p", "--previous", required=True,
                      help="Previous workflow, check for success before submitting current")
  parser.add_argument("-i", "--interval", required=True,
                      help="Submission interval, in minutes")
  parser.add_argument("-n", "--note", required=False, default=None,
                      help="Submission comment text")
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

  auto_submit(args.current, args.previous, args.interval * 60, args.note, args.output_log, dry_run=args.dry_run)

  print(help(fiss_api.create_submission))



if __name__ == "__main__":
    main()

