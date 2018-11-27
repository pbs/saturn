from utils import *

rule_prefix = 'update-'

# testing 
rules = get_rules_by_prefix(rule_prefix)
print(get_logs_for_rule(rules[0]))
for rule in rules:
    print(rule['name'], rule['enabled'], rule['schedule'], rule['target_command'])
# latest_log = get_log_streams(log_group_name)[0]
# for line in get_log_for_run(log_group_name, latest_log['logStreamName']):
#     print(time.ctime(line['timestamp']/1000), line['message'])
