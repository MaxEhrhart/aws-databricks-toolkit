import boto3
from botocore.exceptions import ClientError
import json
from ..custom_objects import SandboxResources


def create_policy(policy_name, policy_arn, policy_definition):
    iam_client = boto3.client('iam')

    # Verificar se a política já existe
    try:
        response = iam_client.get_policy(PolicyArn=f"arn:aws:iam::365143424599:policy/{policy_name}")
        print(f"A política '{policy_name}' já existe.")
        return response['Policy']['Arn']
    except iam_client.exceptions.NoSuchEntityException:
        pass  # A política não existe, então podemos continuar
    except Exception as e:
        print(f"Erro ao verificar a existência da política: {e}")
        return None

    # Criando a política
    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=policy_definition
        )
        print(response)
        policy_arn = response['Policy']['Arn']
        print(f"Política '{policy_name}' criada com sucesso. ARN: {policy_arn}")
        return policy_arn
    except Exception as e:
        print(f"Erro ao criar a política: {e}")
        return None


def check_iam_role_exists(role_name):
    try:
        iam_client = boto3.client('iam')
        iam_client.get_role(RoleName=role_name)
        print(f"Função IAM {role_name} já existe.")
        return True
    except iam_client.exceptions.NoSuchEntityException:
        return False
    except Exception as e:
        print(f"Erro ao verificar a função IAM: {e}")
        return False


# Função para criar uma função IAM
def create_iam_role(role_name, assume_role_policy_document):
    iam_client = boto3.client('iam')

    if check_iam_role_exists(role_name):
        return None

    try:
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_document),
            Description='Descrição da função IAM'
        )
        return response['Role']
    except Exception as e:
        print(f"Erro ao criar a função IAM: {e}")
        return None


# Função para criar um instance profile
def create_instance_profile(instance_profile_name):
    iam_client = boto3.client('iam')

    try:
        print(f'Criando instance-profile {instance_profile_name}')
        response = iam_client.create_instance_profile(
            InstanceProfileName=instance_profile_name
        )
        return response['InstanceProfile']
    except Exception as e:
        message = str(e)
        if 'EntityAlreadyExists' in message:
            print(f'instance-profile {instance_profile_name} já existe.')
            return None
        else:
            print(f"Erro ao criar o instance profile: {e}")
            return None


# Função para anexar a função IAM ao instance profile
def attach_role_to_instance_profile(instance_profile_name, role_name):
    iam_client = boto3.client('iam')

    try:
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=role_name
        )
        return True
    except Exception as e:
        print(f"Erro ao anexar a função IAM ao instance profile: {e}")
        return False


# Função para verificar se uma política está anexada a uma função
def check_policy_attached(role_name, policy_arn):
    try:
        iam_client = boto3.client('iam')
        response = iam_client.list_attached_role_policies(RoleName=role_name)
        for policy in response['AttachedPolicies']:
            if policy['PolicyArn'] == policy_arn:
                print(f"Política {policy_arn} já está anexada à função {role_name}.")
                return True
        return False
    except Exception as e:
        print(f"Erro ao verificar políticas anexadas: {e}")
        return False


# Função para anexar uma política a uma função IAM
def attach_policy_to_role(role_name, policy_arn):
    if check_policy_attached(role_name, policy_arn):
        return

    try:
        iam_client = boto3.client('iam')
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        print(f"Política {policy_arn} anexada à função {role_name}.")
    except Exception as e:
        print(f"Erro ao anexar a política: {e}")


# Função para verificar se um instance profile existe
def check_instance_profile_exists(instance_profile_name):
    try:
        iam_client.get_instance_profile(InstanceProfileName=instance_profile_name)
        print(f"Instance Profile {instance_profile_name} já existe.")
        return True
    except iam_client.exceptions.NoSuchEntityException:
        return False
    except Exception as e:
        print(f"Erro ao verificar o Instance Profile: {e}")
        return False

    # Função para verificar se uma função está associada a um instance profile


def check_role_in_instance_profile(instance_profile_name, role_name):
    try:
        iam_client = boto3.client('iam')
        response = iam_client.get_instance_profile(InstanceProfileName=instance_profile_name)
        for role in response['InstanceProfile']['Roles']:
            if role['RoleName'] == role_name:
                print(f"Função {role_name} já está associada ao Instance Profile {instance_profile_name}.")
                return True
        return False
    except Exception as e:
        print(f"Erro ao verificar função no Instance Profile: {e}")
        return False


# Função para associar uma função a um Instance Profile existente
def add_role_to_instance_profile(instance_profile_name, role_name):
    if check_role_in_instance_profile(instance_profile_name, role_name):
        return

    try:
        iam_client = boto3.client('iam')
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=role_name
        )
        print(f"Função {role_name} adicionada ao Instance Profile {instance_profile_name}.")
    except Exception as e:
        print(f"Erro ao adicionar a função ao Instance Profile: {e}")


# Função para obter a política inline da função
def get_inline_policy(role, policy_name):
    try:
        iam_client = boto3.client('iam')
        policy = iam_client.get_role_policy(
            RoleName=role,
            PolicyName=policy_name
        )
        return policy['PolicyDocument']
    except Exception as e:
        print(f"Erro ao obter a política inline: {e}")
        return None


# Função para atualizar a política
def update_policy(policy_document, instance_profile_arn):
    for statement in policy_document['Statement']:
        if statement.get('Sid') == 'VisualEditor2':
            resources = statement['Resource']
            # Adicionar ARN da Instance Profile
            resources.append(instance_profile_arn)
            # Remover duplicatas e ordenar os recursos
            resources = sorted(set(resources))
            # Atualizar a lista de recursos na política
            statement['Resource'] = resources
            break
    return policy_document


# Função para atualizar a política inline da função
def put_inline_policy(role, policy_name, policy_document):
    try:
        iam_client = boto3.client('iam')
        iam_client.put_role_policy(
            RoleName=role,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document)
        )
        print("Política inline atualizada com sucesso.")
    except Exception as e:
        print(f"Erro ao atualizar a política inline: {e}")