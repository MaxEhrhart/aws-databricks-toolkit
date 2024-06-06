from collections import namedtuple

# Definição do namedtuple para recursos do sandbox
SandboxResources = namedtuple('SandboxResources', [
    'area_name',
    'bucket_name',
    'bucket_arn',
    'policy_name_unity_catalog',
    'policy_arn_unity_catalog',
    'policy_definition_unity_catalog',
    'policy_name_instance_profile',
    'policy_arn_instance_profile',
    'policy_definition_instance_profile',
    'function_name_unity_catalog',
    'function_arn_unity_catalog',
    'function_assume_role_unity_catalog',
    'function_name_instance_profile',
    'function_arn_instance_profile',
    'function_assume_role_instance_profile',
    'instance_profile_arn',
    'policy_cluster_acronym'
])