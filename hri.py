from __future__ import annotations

import datetime
import ipaddress
import json
import pprint
import time
import threading
import concurrent.futures
import boto3
from botocore.exceptions import ClientError
from pytz import timezone

# from cur import get_hri_data_with_cost


class HighRiskItem:
    data = dict()
    IDENTIFIER = "identifier"
    TYPE = "type"
    HRI_IF_STATUS_IS = "hri_if_status_is"

    hri_resource_id_dict = dict()
    # resource id : list of hri key names

    def add_to_hri_data(
        self, hri_data_key, table_row_list, identifier, is_cost_hri=True
    ):
        if is_cost_hri:
            for row in table_row_list:
                row["cost"] = 0

        self.data[hri_data_key] = {
            "count": len(table_row_list),
            "details": table_row_list,
            self.IDENTIFIER: identifier,
        }
        if is_cost_hri:
            self.data[hri_data_key]["total_cost"] = 0

        self.add_resource_id_to_dict(
            table_row_list=table_row_list,
            identifier=identifier,
            hri_data_key=hri_data_key,
        )

    def add_resource_id_to_dict(self, table_row_list, identifier, hri_data_key):
        for row in table_row_list:
            if identifier is not None:
                resource_id = row[identifier]
                if resource_id not in self.hri_resource_id_dict:
                    self.hri_resource_id_dict[resource_id] = []
                self.hri_resource_id_dict[resource_id].append(hri_data_key)

    hri_reverse_db_id_dict = {
        "aws_VPC with Flow Logging Disabled": "aws_aaa",
        "aws_Unencrypted AWS S3 buckets": "aws_aab",
        "aws_Users to review for AWS IAM privilege": "aws_aac",
        "aws_Users without MFA": "aws_aad",
        "aws_Root user without MFA": "aws_aae",
        "aws_Unencrypted AWS RDS instances": "aws_aaf",
        "aws_Unencrypted AWS EBS volumes": "aws_aag",
        "aws_AWS RDS with public subnets with open ports": "aws_aah",
        "aws_AWS EC2 with public subnets with open ports": "aws_aai",
        "aws_AWS S3 buckets with public read/write access": "aws_aaj",
        "aws_Disabled AWS Config for AWS Regions": "aws_aak",
        "aws_AWS CloudTrail event log disabled": "aws_aal",
        "aws_Detected usage of root account": "aws_aam",
        "aws_Inactive IAM account key detected": "aws_aan",
        "aws_Detected AWS EC2 instances in Classic EC2-VPCs": "aws_aao",
        "aws_Disabled AWS GuardDuty accounts": "aws_aap",
        "aws_AWS WAF without resource attached": "aws_aaq",
        "aws_AWS Accounts with non configured Amazon Inspector": "aws_aar",
        "aws_Unused IAM Roles": "aws_aas",
        "aws_EBS snapshots with public access": "aws_aat",
        "aws_Active root access key and secret key": "aws_aau",
        "aws_Detected weak password policy": "aws_aav",
        "aws_Disabled CloudTrail File Integrity check": "aws_aaw",
        "aws_Admin User Detected without Explicit Override": "aws_aax",
        "aws_Public Lambda function without exception": "aws_aay",
        "aws_Lambda environment variables without CMK encryption enabled": "aws_aaz",
        "aws_Lambda function without trigger": "aws_aba",
        "aws_Public AMI": "aws_abb",
        "aws_Public RDS Snapshot": "aws_abc",
        "aws_Infrequently accessed S3 buckets": "aws_abd",
        "aws_Underutilized (< 30% capacity on avg for last week) EC2 instances": "aws_abe",
        "aws_Infrequently accessed EFS resource": "aws_abf",
        "aws_Underutilized (<10%) AWS ECS cluster": "aws_abg",
        # from here are cost rules
        "aws_Review S3 storage class": "aws_abh",
        "aws_Review EC2 instance size": "aws_abi",
        "aws_Review RDS instance size": "aws_abj",
        "aws_Underutilized (<30% read/write) DynamoDB tables": "aws_abk",
        "aws_Underutilized AWS CloudWatch Log Group": "aws_abl",
        "aws_Underutilized AWS EBS provisioned IOPS": "aws_abm",
        "aws_Underutilized AWS RDS provisioned IOPS": "aws_abn",
        "aws_Disabled autoscaling DynamoDB tables": "aws_abo",
        "aws_Unused AWS EBS volumes": "aws_abp",
        "aws_Unused AWS ELB resources": "aws_abq",
        "aws_Unused AWS NAT resources": "aws_abr",
        "aws_Unused AWS Elastic IP (EIP) resources": "aws_abs",
        "aws_Low traffic AWS EC2 instances": "aws_abt",
        "aws_RDS instance idle": "aws_abu",
        "aws_Unattached Workspace Directory": "aws_abv",
        # till here are cost
        "aws_Disabled bucket versioning": "aws_abw",
        "aws_Missing snapshots for AWS EBS volumes": "aws_abx",
        "aws_Disabled AWS DynamoDB backup": "aws_aby",
        "aws_Disabled point-in-time recovery for AWS RDS instance": "aws_abz",
        "aws_Autoscaling disabled for EC2 instances": "aws_aca",
        "aws_Disabled Multi-AZ RDS instances": "aws_acb",
        "aws_Disabled Multi-AZ Elasticache instances": "aws_acc",
        "aws_Disabled AWS Enterprise support": "aws_acd",
        "aws_Out of date AMIs": "aws_ace",
        "aws_Overlapping VPC CIDR": "aws_acf",
        "aws_Missing Tags for EC2 resource": "aws_acg",
        "aws_Missing Tags for EBS resources": "aws_ach",
        "aws_EC2 Scheduled maintenance events": "aws_aci",
        "aws_Underutilized Redshift Cluster Nodes": "aws_acj",
    }
    # AWS_CREDS = {
        
    # }
    # AWS_CREDS = dict()

    def s3(self, region: str):
        """
        'policies' : list_buckets, get_bucket_acl, get_bucket_versioning
        """

        client = boto3.client("s3", region_name=region, **self.AWS_CREDS)

        bucket_tags_cache = {}
        ###################################
        # Find Unencrypted AWS S3 buckets #
        ###################################
        unencrypted_buckets = []

        try:
            response_list_buckets = client.list_buckets()
            for bucket in response_list_buckets.get("Buckets", []):
                bucket_name = bucket.get("Name")
                bucket_tags = []
                if bucket_name not in bucket_tags_cache:
                    try:
                        bucket_tags = client.get_bucket_tagging(
                            Bucket=bucket_name,
                        )["TagSet"]
                    except ClientError as e:
                        continue
                    bucket_tags_cache[bucket_name] = bucket_tags
                else:
                    bucket_tags = bucket_tags_cache[bucket_name]
                try:
                    client.get_bucket_encryption(Bucket=bucket_name)
                except ClientError as e:
                    if (
                        e.response.get("Error", {}).get("Code", "")
                        == "ServerSideEncryptionConfigurationNotFoundError"
                    ):
                        unencrypted_buckets.append(
                            {
                                "bucket_name": bucket_name,
                                "region_name": region,
                                "tags": bucket_tags,
                            }
                        )
        except ClientError as e:
            print(e)
            return

        if unencrypted_buckets:
            self.data["Unencrypted AWS S3 buckets"] = {
                "count": len(unencrypted_buckets),
                "details": unencrypted_buckets,
                self.IDENTIFIER: "bucket_name",
            }

        #####################################################
        # Find AWS S3 buckets with public read/write access #
        #####################################################
        public_bucket_names = []

        for bucket in response_list_buckets.get("Buckets", []):
            bucket_name = bucket.get("Name")

            try:
                public_access_block = client.get_public_access_block(Bucket=bucket_name)
            except ClientError as e:
                if (
                    e.response.get("Error", {}).get("Code", "")
                    == "NoSuchPublicAccessBlockConfiguration"
                ):
                    # Fake public_access_block
                    public_access_block = {
                        "PublicAccessBlockConfiguration": {
                            "BlockPublicAcls": False,
                            "BlockPublicPolicy": False,
                            "IgnorePublicAcls": False,
                            "RestrictPublicBuckets": False,
                        }
                    }
                else:
                    print(e)
                    continue

            public_access_block_config = public_access_block[
                "PublicAccessBlockConfiguration"
            ]

            # Bucket ACLs
            is_bucket_acl_public = False
            if not public_access_block_config["BlockPublicAcls"]:
                if not public_access_block_config["IgnorePublicAcls"]:
                    bucket_acl = client.get_bucket_acl(Bucket=bucket_name)

                    for grant in bucket_acl.get("Grants", []):
                        grantee = grant.get("Grantee", {})

                        if grantee.get("Type", "") == "Group" and grantee.get(
                            "URI", ""
                        ) in [
                            "http://acs.amazonaws.com/groups/global/AuthenticatedUsers",
                            "http://acs.amazonaws.com/groups/global/AllUsers",
                        ]:
                            is_bucket_acl_public = True
                            break

            if is_bucket_acl_public:
                bucket_tags = []
                if bucket_name not in bucket_tags_cache:
                    try:
                        bucket_tags = client.get_bucket_tagging(
                            Bucket=bucket_name,
                        )["TagSet"]
                    except ClientError as e:
                        continue
                    bucket_tags_cache[bucket_name] = bucket_tags
                else:
                    bucket_tags = bucket_tags_cache[bucket_name]
                public_bucket_names.append(
                    {
                        "bucket_name": bucket_name,
                        "region_name": region,
                        "tags": bucket_tags,
                    }
                )
                continue

            # Bucket Policy
            is_bucket_policy_public = False
            if not public_access_block_config["BlockPublicPolicy"]:
                if not public_access_block_config["RestrictPublicBuckets"]:
                    try:
                        bucket_policy_status = client.get_bucket_policy_status(
                            Bucket=bucket_name
                        )
                    except ClientError as e:
                        if (
                            e.response.get("Error", {}).get("Code", "")
                            == "NoSuchBucketPolicy"
                        ):
                            # Does not exist because bucket policy not present
                            # This implies that Bucket Policy is not public
                            bucket_policy_status = {"PolicyStatus": {"IsPublic": False}}
                        else:
                            print(e)
                            continue

                    if bucket_policy_status["PolicyStatus"]["IsPublic"]:
                        is_bucket_policy_public = True

            if is_bucket_policy_public:
                bucket_tags = []
                if bucket_name not in bucket_tags_cache:
                    try:
                        bucket_tags = client.get_bucket_tagging(
                            Bucket=bucket_name,
                        )["TagSet"]
                    except ClientError as e:
                        continue
                    bucket_tags_cache[bucket_name] = bucket_tags
                else:
                    bucket_tags = bucket_tags_cache[bucket_name]
                public_bucket_names.append(
                    {
                        "bucket_name": bucket_name,
                        "region_name": region,
                        "tags": bucket_tags,
                    }
                )

        if public_bucket_names:
            self.data["AWS S3 buckets with public read/write access"] = {
                "count": len(public_bucket_names),
                "details": public_bucket_names,
                self.IDENTIFIER: "bucket_name",
            }

        ##############################
        # Disabled bucket versioning #
        ##############################
        disabled_buckets_versioning = []

        for bucket in response_list_buckets.get("Buckets", []):
            bucket_name = bucket.get("Name")

            try:
                response = client.get_bucket_versioning(Bucket=bucket_name)
                bucket_tags = []
                if bucket_name not in bucket_tags_cache:
                    try:
                        bucket_tags = client.get_bucket_tagging(
                            Bucket=bucket_name,
                        )["TagSet"]
                    except ClientError as e:
                        continue
                    bucket_tags_cache[bucket_name] = bucket_tags
                else:
                    bucket_tags = bucket_tags_cache[bucket_name]
                # Check if versioning is disabled
                bucket_versioning_status = response.get("Status", "")
                if bucket_versioning_status != "Enabled":
                    disabled_buckets_versioning.append(
                        {
                            "bucket_name": bucket_name,
                            "region_name": region,
                            "tags": bucket_tags,
                        }
                    )

            except ClientError as e:
                print(e)

        if disabled_buckets_versioning:
            self.data["Disabled bucket versioning"] = {
                "count": len(disabled_buckets_versioning),
                "details": disabled_buckets_versioning,
                self.IDENTIFIER: "bucket_name",
            }

        ################################################
        # Find infrequently used AWS S3 buckets (Cost) #
        ################################################
        def find_infrequently_used_s3_buckets():
            date_before_60_days = datetime.datetime.now(
                timezone("UTC")
            ) - datetime.timedelta(days=60)
            infrequently_used_bucket_names = []
            for bucket in response_list_buckets.get("Buckets", []):
                bucket_name = bucket.get("Name")
                objects = client.list_objects(Bucket=bucket_name)
                if "Contents" in objects:
                    for obj in objects["Contents"]:
                        key = obj["Key"]
                        curr_bucket_last_used_date = obj["LastModified"]
                        if curr_bucket_last_used_date < date_before_60_days:
                            bucket_tags = []
                            if bucket_name not in bucket_tags_cache:
                                try:
                                    bucket_tags = client.get_bucket_tagging(
                                        Bucket=bucket_name,
                                    )["TagSet"]
                                except ClientError as e:
                                    continue
                                bucket_tags_cache[bucket_name] = bucket_tags
                            else:
                                bucket_tags = bucket_tags_cache[bucket_name]
                            infrequently_used_bucket_names.append(
                                {
                                    "bucket_name": bucket_name,
                                    "last_used_date": curr_bucket_last_used_date,
                                    "region_name": region,
                                    "tags": bucket_tags,
                                }
                            )
                            break

            if infrequently_used_bucket_names:
                self.data["Infrequently used AWS S3 buckets"] = {
                    "count": len(infrequently_used_bucket_names),
                    "details": infrequently_used_bucket_names,
                    self.IDENTIFIER: "bucket_name",
                }

        find_infrequently_used_s3_buckets()

    def iam(self, region: str):
        """
        'policies' : list_users, list_mfa_devices, list_access_keys, get_access_key_last_used,
                     list_attached_user_policies, list_user_policies, get_account_summary,
                     get_role,get_account_password_policy, get_credential_report,
                     generate_credential_report
        """
        client = boto3.client("iam", region_name=region, **self.AWS_CREDS)

        vpc = "-"
        iam_users = []

        list_users_paginator = client.get_paginator("list_users")
        list_users_response_iterator = list_users_paginator.paginate()

        for page in list_users_response_iterator:
            for user in page["Users"]:
                iam_users.append(user["UserName"])

        #####################################
        # Find Users without MFA (Security) #
        #####################################
        users_without_mfa = []

        for iam_user in iam_users:
            try:
                # Pagination not required because we only have to check
                # if the user has at least one MFA device
                response = client.list_mfa_devices(UserName=iam_user)
                if not response["MFADevices"]:
                    users_without_mfa.append({"username": iam_user})
            except ClientError as e:
                if e.response.get("Error", {}).get("Code", "") == "NoSuchEntity":
                    users_without_mfa.append(
                        {"username": iam_user, "vpc": vpc, "region": region}
                    )
                else:
                    print(e)
                    continue

        if users_without_mfa:
            self.data["Users without MFA"] = {
                "count": len(users_without_mfa),
                "details": users_without_mfa,
                self.IDENTIFIER: "username",
            }

        ################################################
        # Inactive IAM account key detected (Security) #
        ################################################
        inactive_keys = []

        def add_inactive_keys_to_list(list_access_keys_data: dict):
            if list_access_keys_data["AccessKeyMetadata"]:
                for access_key_details in list_access_keys_data["AccessKeyMetadata"]:
                    if access_key_details["Status"] == "Inactive":
                        inactive_keys.append(access_key_details["AccessKeyId"])

        for iam_user in iam_users:
            list_access_keys_paginator = client.get_paginator("list_access_keys")
            list_access_keys_paginator_response_iterator = (
                list_access_keys_paginator.paginate(UserName=iam_user)
            )

            for page in list_access_keys_paginator_response_iterator:
                add_inactive_keys_to_list(list_access_keys_data=page)

        if inactive_keys:
            inactive_keys_for_last_6_days = []
            for inactive_key in inactive_keys:
                response = client.get_access_key_last_used(AccessKeyId=inactive_key)
                localtz = timezone("utc")
                dt_aware = localtz.localize(
                    datetime.datetime.now() - datetime.timedelta(days=6)
                )
                if response["AccessKeyLastUsed"]["LastUsedDate"] < dt_aware:
                    inactive_keys_for_last_6_days.append(
                        {
                            "vpc": vpc,
                            "region": region,
                            "key": inactive_key,
                            "username": response["UserName"],
                            "last_used_date": response["AccessKeyLastUsed"][
                                "LastUsedDate"
                            ],
                        }
                    )

            if inactive_keys_for_last_6_days:
                # Inactive keys for last 6 days
                self.data["Inactive IAM account key detected"] = {
                    "count": len(inactive_keys_for_last_6_days),
                    "details": inactive_keys_for_last_6_days,
                    self.IDENTIFIER: "key",
                }

        #########################################
        # Users to review for AWS IAM privilege #
        #########################################
        list_of_user_policy = []

        for iam_user in iam_users:
            attached_policy_names = []
            inline_policy_names = []

            try:
                # Retrieve the list of policies attached to the user
                list_attached_user_policies_paginator = client.get_paginator(
                    "list_attached_user_policies"
                )
                list_attached_user_policies_response_iterator = (
                    list_attached_user_policies_paginator.paginate(UserName=iam_user)
                )

                for page in list_attached_user_policies_response_iterator:
                    if page["AttachedPolicies"]:
                        attached_policy_names = [
                            policy["PolicyName"] for policy in page["AttachedPolicies"]
                        ]
            except ClientError as e:
                print(e)

            try:
                # Retrieve the list of inline policies to the user
                list_user_policies_paginator = client.get_paginator(
                    "list_user_policies"
                )
                list_user_policies_response_iterator = (
                    list_user_policies_paginator.paginate(UserName=iam_user)
                )

                for page in list_user_policies_response_iterator:
                    if page["PolicyNames"]:
                        inline_policy_names = [policy for policy in page["PolicyNames"]]
            except ClientError as e:
                print(e)

            if attached_policy_names or inline_policy_names:
                list_of_user_policy.append(
                    {
                        "vpc": vpc,
                        "region": region,
                        "username": iam_user,
                        "attached_policies": attached_policy_names,
                        "inline_policies": inline_policy_names,
                    }
                )

        if list_of_user_policy:
            # list_of_user_policy
            self.data["Users to review for AWS IAM privilege"] = {
                "count": len(list_of_user_policy),
                "details": list_of_user_policy,
                self.IDENTIFIER: "username",
            }

        ####################################
        # Root user without MFA (Security) #
        ####################################
        root_user_without_mfa = False

        try:
            # Get the account summary
            summary = client.get_account_summary()
            # Check if MFA is enabled for the root user
            mfa_enabled = summary["SummaryMap"]["AccountMFAEnabled"]

            if not mfa_enabled:
                root_user_without_mfa = True
        except ClientError as e:
            print(e)

        if root_user_without_mfa:
            self.data["Root user without MFA"] = {
                "count": 1,
                "details": [{"status": True, "vpc": vpc, "region": region}],
                self.IDENTIFIER: "status",
                self.TYPE: "boolean",
                self.HRI_IF_STATUS_IS: True,
            }

        ###############################
        # Unused IAM Roles (Security) #
        ###############################
        unused_roles = []

        list_roles_paginator = client.get_paginator("list_roles")
        list_roles_response_iterator = list_roles_paginator.paginate()

        localtz = timezone("utc")
        dt_aware = localtz.localize(
            datetime.datetime.now() - datetime.timedelta(days=90)
        )

        # Check if the role has not been used in the past 90 days or empty lastUsedDate
        for page in list_roles_response_iterator:
            if page:
                for role in page["Roles"]:
                    try:
                        response = client.get_role(RoleName=role["RoleName"])
                        last_used = response["Role"]["RoleLastUsed"]
                        if last_used:
                            last_used_date = last_used["LastUsedDate"]

                            if last_used_date < dt_aware:
                                unused_roles.append(
                                    {
                                        "role_name": role["RoleName"],
                                        "vpc": vpc,
                                        "region": region,
                                    }
                                )
                        else:
                            unused_roles.append(
                                {
                                    "role_name": role["RoleName"],
                                    "vpc": vpc,
                                    "region": region,
                                }
                            )
                    except ClientError as e:
                        print(e)

        if unused_roles:
            self.data["Unused IAM Roles"] = {
                "count": len(unused_roles),
                "details": unused_roles,
                self.IDENTIFIER: "role_name",
            }

        ##########################################################
        # Admin User Detected without Explicit Override-security #
        ##########################################################
        administrator_access_user = []

        for user_policy_data in list_of_user_policy:
            if "AdministratorAccess" in user_policy_data.get("attached_policies", []):
                administrator_access_user.append(
                    {
                        "username": user_policy_data.get("username"),
                        "vpc": vpc,
                        "region": region,
                    }
                )

        if administrator_access_user:
            self.data["Admin User Detected without Explicit Override"] = {
                "count": len(administrator_access_user),
                "details": administrator_access_user,
                self.IDENTIFIER: "username",
            }

        ############################################
        # Detected weak password policy (Security) #
        ############################################
        detected_weak_password_policy = False

        try:
            policy = client.get_account_password_policy()
            if (
                policy["MinimumPasswordLength"] < 6
                or not policy["RequireUppercaseCharacters"]
                or not policy["RequireLowercaseCharacters"]
                or not policy["RequireNumbers"]
                or not policy["RequireSymbols"]
                or not policy["ExpirePasswords"]
                or not policy["AllowUsersToChangePassword"]
                or policy["HardExpiry"]
                or policy["PasswordReusePrevention"] != 1
                or policy["MaxPasswordAge"] > 90
            ):
                detected_weak_password_policy = True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code", "") == "NoSuchEntity":
                detected_weak_password_policy = True
            else:
                print(e)

        if detected_weak_password_policy:
            self.data["Detected weak password policy"] = {
                "count": 1,
                "details": [{"status": True, "vpc": vpc, "region": region}],
                self.IDENTIFIER: "status",
                self.TYPE: "boolean",
                self.HRI_IF_STATUS_IS: True,
            }

        ####################################################
        # Active root access key and secret key (Security) #
        ####################################################
        active_root_access_key_and_secret_key = False
        credential_report = None

        try:
            credential_report = client.get_credential_report()
        except ClientError as e:
            error = e.response.get("Error", {}).get("Code", "")
            if error == "ReportNotPresent":
                # Generate Report
                client.generate_credential_report()

                while True:
                    time.sleep(2)
                    try:
                        credential_report = client.get_credential_report()
                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code")
                        if error_code != "ReportNotPresent":
                            print(e)
                            break

                    if credential_report is not None:
                        break

                # credential_report = client.get_credential_report()
            else:
                print(e)

        if credential_report is not None:
            credential_report_content = credential_report["Content"].decode()
            credential_report_lines = credential_report_content.splitlines()
            credential_report_header = credential_report_lines[0]
            credential_report_data = credential_report_lines[1:2]

            # index of the "access_key_1_active" and "access_key_2_active" fields
            access_key_1_active_index = credential_report_header.split(",").index(
                "access_key_1_active"
            )
            access_key_2_active_index = credential_report_header.split(",").index(
                "access_key_2_active"
            )

            for line in credential_report_data:
                fields = line.split(",")
                access_key_1_active = fields[access_key_1_active_index]
                access_key_2_active = fields[access_key_2_active_index]
                if access_key_1_active == "true" or access_key_2_active == "true":
                    active_root_access_key_and_secret_key = True

        if active_root_access_key_and_secret_key:
            self.data["Active root access key and secret key"] = {
                "count": 1,
                "details": [{"status": True, "vpc": vpc, "region": region}],
                self.IDENTIFIER: "status",
                self.TYPE: "boolean",
                self.HRI_IF_STATUS_IS: True,
            }

        #############################################
        # Detected usage of root account (Security) #
        #############################################
        detected_usage_of_root_account = False

        now = datetime.datetime.now()
        thirty_days_ago = now - datetime.timedelta(days=30)
        if credential_report is not None:
            credential_report_content = credential_report["Content"].decode()
            credential_report_lines = credential_report_content.splitlines()
            credential_report_header = credential_report_lines[0]
            credential_report_data = credential_report_lines[1:2]

            password_last_used_index = credential_report_header.split(",").index(
                "password_last_used"
            )

            for line in credential_report_data:
                fields = line.split(",")
                password_last_used = fields[password_last_used_index]
                if password_last_used >= thirty_days_ago.isoformat():
                    detected_usage_of_root_account = True

        if detected_usage_of_root_account:
            self.data["Detected usage of root account"] = {
                "count": 1,
                "details": [{"status": True, "vpc": vpc, "region": region}],
                self.IDENTIFIER: "status",
                self.TYPE: "boolean",
                self.HRI_IF_STATUS_IS: True,
            }

        return self.data

    def config(self, region: str):
        """
        'policies' :describe_delivery_channels, describe_configuration_recorder_status
        """
        data = {}
        client = boto3.client("config", region_name=region, **self.AWS_CREDS)

        #######################################
        # Disabled aws config for aws Regions #
        #######################################
        config_status = []

        try:
            delivery_channels = client.describe_delivery_channels()
            response = client.describe_configuration_recorder_status()
            status = response["ConfigurationRecordersStatus"]
            if "recording" in status and len(delivery_channels) >= 0:
                if not status["recording"]:
                    config_status.append({"status": True})
            else:
                config_status.append({"status": True, "aws_region": region})
        except ClientError as e:
            print(e)

        if config_status:
            self.data["Disabled AWS Config for AWS Regions"] = {
                "count": 1,
                "details": config_status,
                self.IDENTIFIER: "status",
                self.TYPE: "boolean",
                self.HRI_IF_STATUS_IS: True,
            }

    def ec2(self, region: str):
        """
        'policies' :describe_vpcs, describe_flow_logs, describe_instances, describe_volumes,
                    describe_snapshots, describe_snapshot_attribute, describe_auto_scaling_groups,
                    describe_instance_status, describe_images, describe_addresses, describe_security_groups
        """
        client = boto3.client("ec2", region_name=region, **self.AWS_CREDS)

        #############################################
        # Vpc with flow logging disabled (Security) #
        #############################################
        vpc_with_flow_log_disabled = []
        vpc_name = []

        # list of all vpcs in the account
        describe_vpcs_paginator = client.get_paginator("describe_vpcs")
        describe_vpcs_response_iterator = describe_vpcs_paginator.paginate()

        for page in describe_vpcs_response_iterator:
            if page:
                for vpc in page["Vpcs"]:
                    flow_log_filter = {"Name": "resource-id", "Values": [vpc["VpcId"]]}
                    if "Tags" in vpc:
                        for tag in vpc["Tags"]:
                            if tag["Key"] == "Name":
                                vpc_name = tag["Value"]
                                break
                        else:
                            vpc_name = "-"
                    else:
                        vpc_name = "-"
                    try:
                        response = client.describe_flow_logs(Filters=[flow_log_filter])
                        if len(response["FlowLogs"]) == 0:
                            vpc_with_flow_log_disabled.append(
                                {
                                    "vpc_id": vpc["VpcId"],
                                    "vpc": vpc_name,
                                    "region": region,
                                }
                            )

                    except ClientError as e:
                        print(e)

        if vpc_with_flow_log_disabled:
            self.data["VPC with Flow Logging Disabled"] = {
                "count": len(vpc_with_flow_log_disabled),
                "details": vpc_with_flow_log_disabled,
                self.IDENTIFIER: "vpc_id",
            }

        #############################################################
        # Detected AWS EC2 instances in Classic EC2-VPCs (Security) #
        #############################################################
        ec2_classic_vpc = []

        describe_instances_paginator = client.get_paginator("describe_instances")
        describe_instances_page_iterator = describe_instances_paginator.paginate()

        for page in describe_instances_page_iterator:
            for reservation in page["Reservations"]:
                for instance in reservation["Instances"]:
                    if "Tags" in instance:
                        for tag in instance["Tags"]:
                            if tag["Key"] == "Name":
                                instance_name = tag["Value"]
                                break
                        else:
                            instance_name = "-"
                    else:
                        instance_name = "-"

                    if "VpcId" not in instance.keys():
                        ec2_classic_vpc.append(
                            {
                                "instance_name": instance_name,
                                "instance_id": instance["InstanceId"],
                                "vpc_id": instance["VpcId"],
                                "subnet_id": instance["SubnetId", "region":region],
                            }
                        )

        if ec2_classic_vpc:
            self.data["Detected AWS EC2 instances in Classic EC2-VPCs"] = {
                "count": len(ec2_classic_vpc),
                "details": ec2_classic_vpc,
                self.IDENTIFIER: "instance_id",
            }

        ##########################################
        # Unencrypted AWS EBS volumes (Security) #
        ##########################################
        unencrypted_ebs_volumes = []

        describe_volumes_paginator = client.get_paginator("describe_volumes")
        describe_volumes_response_iterator = describe_volumes_paginator.paginate()

        for page in describe_volumes_response_iterator:
            for volume in page["Volumes"]:
                if not volume["Encrypted"]:
                    instanceid = volume["Attachments"][0]["InstanceId"]
                    volume_tag = "-"
                    if "Tags" in volume:
                        for tag in volume["Tags"]:
                            if tag["Key"] == "Name":
                                volume_tag = tag["Value"]
                                break
                            else:
                                volume_tag = "-"
                    else:
                        volume_tag = "-"
                    unencrypted_ebs_volumes.append(
                        {
                            "volume_name": volume_tag,
                            "volume_id": volume["VolumeId"],
                            "AvailabilityZone": volume["AvailabilityZone"],
                            "Instance_id": instanceid,
                            "region": region,
                        }
                    )

        if unencrypted_ebs_volumes:
            self.data["Unencrypted AWS EBS volumes"] = {
                "count": len(unencrypted_ebs_volumes),
                "details": unencrypted_ebs_volumes,
                self.IDENTIFIER: "volume_id",
            }

        # ###############################################
        # # EBS snapshots with public access (Security) #
        # ###############################################
        snapshot_with_public_access = []

        describe_snapshots_paginator = client.get_paginator("describe_snapshots")
        describe_snapshots_response_iterator = describe_snapshots_paginator.paginate(
            OwnerIds=["self"]
        )

        for page in describe_snapshots_response_iterator:
            for snapshot in page.get("Snapshots", []):
                if "Tags" in snapshot:
                    tags = snapshot["Tags"]
                    name_found = False
                    for tag in tags:
                        if tag["Key"] == "Name":
                            snapshot_name = tag["Value"]
                            name_found = True
                            break
                    if not name_found:
                        snapshot_name = "-"
                else:
                    snapshot_name = "-"

                try:
                    result = client.describe_snapshot_attribute(
                        SnapshotId=snapshot["SnapshotId"],
                        Attribute="createVolumePermission",
                    )

                    for permission in result.get("CreateVolumePermissions", []):
                        if permission["Group"] == "all":
                            snapshot_with_public_access.append(
                                {
                                    "snapshot_name": snapshot_name,
                                    "snapshot_id": snapshot["SnapshotId"],
                                    "volume_id": snapshot["VolumeId"],
                                    "region": region,
                                }
                            )
                            break
                except ClientError as e:
                    print(e)

        if snapshot_with_public_access:
            self.data["EBS snapshots with public access"] = {
                "count": len(snapshot_with_public_access),
                "details": snapshot_with_public_access,
                self.IDENTIFIER: "snapshot_id",
            }

        # #######################################################
        # # Missing snapshots for AWS EBS volumes (Reliability) #
        # #######################################################
        volumes_without_snapshots = []

        volumes_with_snapshots = set()
        for page in describe_snapshots_response_iterator:
            for snapshot in page.get("Snapshots", []):
                volume_id = snapshot.get("VolumeId", None)
                if volume_id:
                    volumes_with_snapshots.add(volume_id)

        for page in describe_volumes_response_iterator:
            for volume in page["Volumes"]:
                volume_id = volume["VolumeId"]

                instanceid = volume["Attachments"][0]["InstanceId"]
                volume_tag = "-"
                if "Tags" in volume:
                    for tag in volume["Tags"]:
                        if tag["Key"] == "Name":
                            volume_tag = tag["Value"]
                            break
                        else:
                            volume_tag = "-"
                else:
                    volume_tag = "-"
                if volume_id not in volumes_with_snapshots:
                    volumes_without_snapshots.append(
                        {
                            "volume_name": volume_tag,
                            "volume_id": volume_id,
                            "AvailabilityZone": volume["AvailabilityZone"],
                            "instance_id": instanceid,
                            "region": region,
                        }
                    )

        if volumes_without_snapshots:
            self.data["Missing snapshots for AWS EBS volumes"] = {
                "count": len(volumes_without_snapshots),
                "details": volumes_without_snapshots,
                self.IDENTIFIER: "volume_id",
            }

        # ########################################################
        # # Autoscaling disabled for EC2 instances (Reliability) #
        # # ########################################################
        instances_without_autoscaling = []
        instances_with_autoscaling = set()

        autoscaling_client = boto3.client("autoscaling", **self.AWS_CREDS)

        describe_auto_scaling_groups_paginator = autoscaling_client.get_paginator(
            "describe_auto_scaling_groups"
        )
        describe_auto_scaling_groups_response_iterator = (
            describe_auto_scaling_groups_paginator.paginate()
        )

        for page in describe_auto_scaling_groups_response_iterator:
            for group in page["AutoScalingGroups"]:
                instances_with_autoscaling.update(
                    [instance["InstanceId"] for instance in group.get("Instances", [])]
                )

        for page in describe_instances_page_iterator:
            for reservation in page["Reservations"]:
                for instance in reservation["Instances"]:
                    if "Tags" in instance:
                        for tag in instance["Tags"]:
                            if tag["Key"] == "Name":
                                instance_name = tag["Value"]
                                break
                        else:
                            instance_name = "-"
                    else:
                        instance_name = "-"

                    if not instance["InstanceId"] in instances_with_autoscaling:
                        instances_without_autoscaling.append(
                            {
                                "instance_name": instance_name,
                                "instance_id": instance["InstanceId"],
                                "vpc_id": instance["VpcId"],
                                "subnet_id": instance["SubnetId"],
                                "region": region,
                            }
                        )

        if instances_without_autoscaling:
            self.data["Autoscaling disabled for EC2 instances"] = {
                "count": len(instances_without_autoscaling),
                "details": instances_without_autoscaling,
                self.IDENTIFIER: "instance_id",
            }

        # # #################################################
        # # # EC2 Scheduled maintenance events (Operations) #
        # # #################################################
        schedule_events_details = []

        for page in describe_instances_page_iterator:
            for reservation in page["Reservations"]:
                for instance in reservation["Instances"]:
                    if "Tags" in instance:
                        for tag in instance["Tags"]:
                            if tag["Key"] == "Name":
                                instance_name = tag["Value"]
                                break
                        else:
                            instance_name = "-"
                    else:
                        instance_name = "-"
                    try:
                        response = client.describe_instance_status(
                            InstanceIds=[instance["InstanceId"]]
                        )
                        for instance_status in response["InstanceStatuses"]:
                            if "Events" in instance_status:
                                for event in instance_status["Events"]:
                                    schedule_events_details.append(
                                        {
                                            "instance_name": instance_name,
                                            "instance_id": instance["InstanceId"],
                                            "vpc_id": instance["VpcId"],
                                            "subnet_id": instance["SubnetId"],
                                            "region": region**event,
                                        }
                                    )
                    except ClientError as e:
                        print(e)

        if schedule_events_details:
            self.data["EC2 Scheduled maintenance events"] = {
                "count": len(schedule_events_details),
                "details": schedule_events_details,
                self.IDENTIFIER: "instance_id",
            }

        # ##############################################
        # # Missing Tags for EC2 resources (Operations) #
        # ##############################################
        missing_tag_instances = []

        for page in describe_instances_page_iterator:
            for reservation in page["Reservations"]:
                for instance in reservation["Instances"]:
                    if len(instance.get("Tags", [])) == 0:
                        missing_tag_instances.append(
                            {
                                "instance_id": instance["InstanceId"],
                                "vpc_id": instance["VpcId"],
                                "subnet_id": instance["SubnetId"],
                                "region": region,
                            }
                        )

        if missing_tag_instances:
            self.data[" Missing Tags for EC2 resources"] = {
                "count": len(missing_tag_instances),
                "details": missing_tag_instances,
                self.IDENTIFIER: "instance_id",
            }

        # ##############################################
        # # Missing Tags for EBS resources (Operations) #
        # ##############################################
        missing_tags_for_ebs_resource = []

        for page in describe_volumes_response_iterator:
            for volume in page["Volumes"]:
                if len(volume.get("Tags", [])) == 0:
                    missing_tags_for_ebs_resource.append(
                        {
                            "volume_id": volume["VolumeId"],
                            "AvailabilityZone": volume["AvailabilityZone"],
                            "region": region,
                        }
                    )

        if missing_tags_for_ebs_resource:
            self.data["Missing Tags for EBS resources"] = {
                "count": len(missing_tags_for_ebs_resource),
                "details": missing_tags_for_ebs_resource,
                self.IDENTIFIER: "volume_id",
            }

        # #################################
        # # Out of date AMIs (Operations) #
        # #################################
        out_of_date_amis = []

        localtz = timezone("utc")
        dt_aware = localtz.localize(
            datetime.datetime.now() - datetime.timedelta(days=180)
        )

        describe_images_paginator = client.get_paginator("describe_images")
        describe_images_response_iterator = describe_images_paginator.paginate(
            Owners=[
                "self",
            ],
        )

        for page in describe_images_response_iterator:
            for image in page.get("Images", []):
                tags = image.get("Tags", [])
                image_name = "-"

                for tag in tags:
                    if tag.get("Key", "") == "Name":
                        image_name = tag.get("Value", "-")
                creation_date = datetime.datetime.fromisoformat(
                    image.get("CreationDate")[:-1]
                ).astimezone(datetime.timezone.utc)
                if dt_aware >= creation_date:
                    out_of_date_amis.append(
                        {
                            "image_name": image_name,
                            "image_id": image["ImageId"],
                            "region": region,
                            "vpc": "-",
                        }
                    )

        if out_of_date_amis:
            self.data["Out of date AMIs"] = {
                "count": len(out_of_date_amis),
                "details": out_of_date_amis,
                self.IDENTIFIER: "image_id",
            }

        # #########################
        # # Public AMI (Security) #
        # #########################
        public_amis = []

        for page in describe_images_response_iterator:
            for image in page.get("Images", []):
                tags = image.get("Tags", [])
                image_name = "-"
                for tag in tags:
                    if tag.get("Key", "") == "Name":
                        image_name = tag.get("Value", "-")
                if image["Public"]:
                    public_amis.append(
                        {
                            "image_name": image_name,
                            "image_id": image["ImageId"],
                            "region": region,
                            "vpc": "-",
                        }
                    )
                    break

        if public_amis:
            self.data["Public AMI"] = {
                "count": len(public_amis),
                "details": public_amis,
                self.IDENTIFIER: "image_id",
            }

        # ########################################
        # # Unused AWS EBS volumes (Performance) #
        # ########################################
        unused_ebs_volumes = []

        for page in describe_volumes_response_iterator:
            for volume in page["Volumes"]:
                volume_tag = "-"
                if "Tags" in volume:
                    for tag in volume["Tags"]:
                        if tag["Key"] == "Name":
                            volume_tag = tag["Value"]
                            break
                        else:
                            volume_tag = "-"
                else:
                    volume_tag = "-"
                if volume["State"] == "available":
                    unused_ebs_volumes.append(
                        {
                            "volume_name": volume_tag,
                            "volume_id": volume["VolumeId"],
                            "AvailabilityZone": volume["AvailabilityZone"],
                            "region": region,
                            "vpc": "-",
                        }
                    )

        if unused_ebs_volumes:
            self.data["Unused AWS EBS volumes"] = {
                "count": len(unused_ebs_volumes),
                "details": unused_ebs_volumes,
                self.IDENTIFIER: "volume_id",
            }

        # #######################################################
        # # Unused AWS Elastic IP (EIP) resources (Performance) #
        # #######################################################
        unused_eips = []

        try:
            # Describe all EIPs
            eips = client.describe_addresses()
            if eips["Addresses"] != []:
                if "Tags" in eips["Addresses"][0]:
                    tags = eips["Addresses"][0]["Tags"]
                    for tag in tags:
                        if tag["Key"] == "Name":
                            eips_name = tag["Value"]
                            break
                    else:
                        eips_name = "-"
                else:
                    eips_name = "-"
            # check any association
            for eip in eips["Addresses"]:
                if "AssociationId" not in eip:
                    unused_eips.append(
                        {
                            "eips_name": eips_name,
                            "allocation_id": eip["AllocationId"],
                            "region": region,
                            "vpc": "-",
                        }
                    )
        except ClientError as e:
            print(e)

        if unused_eips:
            self.data["Unused AWS Elastic IP (EIP) resources"] = {
                "count": len(unused_eips),
                "details": unused_eips,
                self.IDENTIFIER: "allocation_id",
            }

        # ##########################################################
        # # AWS EC2 with public subnets with open ports (Security) #
        # ##########################################################
        ec2_with_public_subnets_with_open_ports = []

        describe_security_groups_paginator = client.get_paginator(
            "describe_security_groups"
        )

        # Get Ids of all security groups with open SSH (22) ports
        group_ids_to_check = set()

        def populate_group_ids_to_check(sec_groups_iterator: dict):
            for sec_group_page in sec_groups_iterator:
                sec_groups = sec_group_page.get("SecurityGroups", [])
                for sec_group in sec_groups:
                    is_ssh_port_open = False
                    for ip_permission in sec_group.get("IpPermissions", []):
                        from_port, to_port = ip_permission.get(
                            "FromPort"
                        ), ip_permission.get("ToPort")
                        if (
                            from_port is not None
                            and to_port is not None
                            and from_port <= 22 <= to_port
                        ):
                            is_ssh_port_open = True
                            break
                        else:
                            if ip_permission.get("IpProtocol") == "-1":
                                is_ssh_port_open = True
                                break

                    if is_ssh_port_open:
                        sec_group_id = sec_group["GroupId"]
                        group_ids_to_check.add(sec_group_id)

        populate_group_ids_to_check(
            describe_security_groups_paginator.paginate(
                Filters=[
                    {"Name": "ip-permission.cidr", "Values": ["0.0.0.0/0"]},
                ]
            )
        )

        populate_group_ids_to_check(
            describe_security_groups_paginator.paginate(
                Filters=[
                    {"Name": "ip-permission.ipv6-cidr", "Values": ["::/0"]},
                ]
            )
        )

        ec2_instances_by_network_interface_group_id = {}
        for page in describe_instances_page_iterator:
            for reservation in page.get("Reservations", []):
                for instance in reservation.get("Instances", []):
                    if "Tags" in instance:
                        for tag in instance["Tags"]:
                            if tag["Key"] == "Name":
                                instance_name = tag["Value"]
                                break
                        else:
                            instance_name = "-"
                    else:
                        instance_name = "-"
                    for network_interface in instance.get("NetworkInterfaces", []):
                        for security_group in network_interface.get("Groups", []):
                            group_id = security_group.get("GroupId")
                            if group_id in group_ids_to_check:
                                if (
                                    group_id
                                    in ec2_instances_by_network_interface_group_id
                                ):
                                    ec2_instances_by_network_interface_group_id[
                                        group_id
                                    ].append(
                                        {
                                            "instance_name": instance_name,
                                            "instance_id": instance["InstanceId"],
                                            "subnet_id": instance["SubnetId"],
                                            "vpc_id": instance["VpcId"],
                                            "region": region,
                                        }
                                    )
                                else:
                                    ec2_instances_by_network_interface_group_id[
                                        group_id
                                    ] = [
                                        {
                                            "instance_name": instance_name,
                                            "instance_id": instance["InstanceId"],
                                            "subnet_id": instance["SubnetId"],
                                            "vpc_id": instance["VpcId"],
                                            "region": region,
                                        }
                                    ]

        # Find out all subnets which are public and related to the ec2 instances with open SSH ports
        describe_route_tables_paginator = client.get_paginator("describe_route_tables")
        describe_route_tables_response_iterator = (
            describe_route_tables_paginator.paginate()
        )

        vpc_detail = dict()
        route_tables_with_igw_attached = set()
        for page in describe_route_tables_response_iterator:
            for route_table in page.get("RouteTables", []):
                vpc_id = route_table.get("VpcId")
                route_table_id = route_table.get("RouteTableId")

                subnet_ids = []
                is_main_route_table = False

                # Find if this route_table is "main"
                for association in route_table.get("Associations", []):
                    subnet_id = association.get("SubnetId", None)
                    if subnet_id is not None:
                        subnet_ids.append(subnet_id)

                    if association.get("Main", False):
                        is_main_route_table = True

                # Group subnets by VpcId mapping route_table (Explicit associations)
                if vpc_id in vpc_detail:
                    if "subnet_detail" in vpc_detail[vpc_id]:
                        for subnet_id in subnet_ids:
                            vpc_detail[vpc_id]["subnet_detail"][
                                subnet_id
                            ] = route_table_id
                    else:
                        vpc_detail[vpc_id]["subnet_detail"] = {
                            subnet_id: route_table_id for subnet_id in subnet_ids
                        }
                else:
                    vpc_detail[vpc_id] = {
                        "subnet_detail": {
                            subnet_id: route_table_id for subnet_id in subnet_ids
                        }
                    }

                if is_main_route_table:
                    vpc_detail[vpc_id]["main_route_table_id"] = route_table_id

                # Find if this route table has Internet Gateway Attached to it
                for route in route_table.get("Routes", []):
                    gid = route.get("GatewayId", "")
                    if gid.startswith("igw-"):
                        route_tables_with_igw_attached.add(route_table_id)
                        break

        # Find all EC2 instance with open SSH port and with public subnet
        for group_id in list(group_ids_to_check):
            ec2_instances = ec2_instances_by_network_interface_group_id.get(
                group_id, []
            )
            for ec2_instance in ec2_instances:
                vpc_id = ec2_instance.get("vpc_id")
                subnet_id = ec2_instance.get("subnet_id")

                # Check explicit Association
                route_table_id = (
                    vpc_detail.get(vpc_id, {})
                    .get("subnet_detail", {})
                    .get(subnet_id, None)
                )

                # Check implicit Association
                if route_table_id is None:
                    route_table_id = vpc_detail.get(vpc_id, {}).get(
                        "main_route_table_id", None
                    )

                if route_table_id is not None:
                    # Check if this route table is attached to InternetGateway
                    if route_table_id in route_tables_with_igw_attached:
                        # This subnet is public
                        ec2_with_public_subnets_with_open_ports.append(
                            {"ec2_instance": ec2_instance, "region": region}
                        )
                else:
                    print(
                        f"Something Went Wrong. Subnet ({subnet_id}) association with route table not found"
                    )

        if ec2_with_public_subnets_with_open_ports:
            self.data["AWS EC2 with public subnets with open ports"] = {
                "count": len(ec2_with_public_subnets_with_open_ports),
                "details": ec2_with_public_subnets_with_open_ports,
                self.IDENTIFIER: "instance_id",
            }

        # #####################################
        # # Overlapping VPC CIDR (Operations) #
        # #####################################
        overlapping_vpc_cidrs = []

        vpc_cidr_blocks = []
        for page in describe_vpcs_response_iterator:
            for vpc in page["Vpcs"]:
                if "Tags" in vpc:
                    tags = vpc["Tags"]
                    for tag in tags:
                        if tag["Key"] == "Name":
                            vpc_name = tag["Value"]
                            break
                    else:
                        vpc_name = "-"
                else:
                    vpc_name = "-"
                vpc_id = vpc["VpcId"]
                cidr_block = vpc["CidrBlock"]

                vpc_cidr_blocks.append(
                    {"vpc_name": vpc_name, "vpc_id": vpc_id, "cidr_block": cidr_block}
                )

        for primary_idx in range(len(vpc_cidr_blocks)):
            for secondary_idx in range(primary_idx + 1, len(vpc_cidr_blocks)):
                primary_vpc_cidr_block = vpc_cidr_blocks[primary_idx]
                primary_vpc_id = primary_vpc_cidr_block.get("vpc_id")
                primary_cidr_block = primary_vpc_cidr_block.get("cidr_block")

                secondary_vpc_cidr_block = vpc_cidr_blocks[secondary_idx]
                secondary_vpc_id = secondary_vpc_cidr_block.get("vpc_id")
                secondary_cidr_block = secondary_vpc_cidr_block.get("cidr_block")

                primary_network = ipaddress.ip_network(primary_cidr_block)
                secondary_network = ipaddress.ip_network(secondary_cidr_block)

                if primary_network.overlaps(secondary_network):
                    overlapping_vpc_cidrs.append(
                        {
                            "region": region,
                            "vpc_id": primary_vpc_id,
                            "cidr_block": primary_cidr_block,
                            "overlapping_vpc_id": secondary_vpc_id,
                            "overlapping_cidr_block": secondary_cidr_block,
                        }
                    )

        if overlapping_vpc_cidrs:
            self.data["Overlapping VPC CIDR"] = {
                "count": len(overlapping_vpc_cidrs),
                "details": overlapping_vpc_cidrs,
                self.IDENTIFIER: ["vpc_id", "overlapping_vpc_id"],  # Unique Together
            }

        return self.data

    def rds(self, region: str):
        """
        'policies' :describe_db_snapshots ,describe_db_snapshot_attributes ,describe_db_instances,
                    describe_route_tables,
        """

        client = boto3.client("rds", region_name=region, **self.AWS_CREDS)

        ##################################
        # Public RDS Snapshot (Security) #
        ##################################
        public_rds_snapshot = []

        try:
            describe_db_snapshots_paginator = client.get_paginator(
                "describe_db_snapshots"
            )
            describe_db_snapshots_page_iterator = (
                describe_db_snapshots_paginator.paginate(SnapshotType="public")
            )

            for page in describe_db_snapshots_page_iterator:
                for snapshot in page["DBSnapshots"]:
                    snapshot_identifier = snapshot["DBSnapshotIdentifier"]
                    vpc_snapshot = snapshot["VpcId"]
                    AvailabilityZone = snapshot["AvailabilityZone"]
                    public_rds_snapshot.append(
                        {
                            "snapshot_identifier": snapshot_identifier,
                            "Vpc_id": vpc_snapshot,
                            "AvailabilityZone": AvailabilityZone,
                            "region": region,
                        }
                    )
        except ClientError as e:
            print(e)

        if public_rds_snapshot:
            self.data["Public RDS Snapshot"] = {
                "count": len(public_rds_snapshot),
                "details": public_rds_snapshot,
                self.IDENTIFIER: "snapshot_identifier",
            }

        ############################################
        # AWS RDS with public subnets with open ports-security
        ############################################

        # Get all RDS instances
        # instances = client.describe_db_instances()
        describe_db_instances_paginator = client.get_paginator("describe_db_instances")
        describe_db_instances_page_iterator = describe_db_instances_paginator.paginate()
        # Connect to EC2 using the boto3 library
        ec2 = boto3.client("ec2", **self.AWS_CREDS)

        rds_with_public_subnets_with_open_ports = []
        rds_with_public_subnets = False
        rds_with_open_port = False
        for page in describe_db_instances_page_iterator:
            for instance in page["DBInstances"]:
                db_instance_identifier = instance["DBInstanceIdentifier"]
                vpc_id = instance["DBSubnetGroup"]["VpcId"]

                subnets = ec2.describe_subnets(
                    Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
                )
                for subnet in subnets["Subnets"]:
                    subnet_id = subnet["SubnetId"]

                    # Get information about the route tables in the VPC
                    route_tables = ec2.describe_route_tables(
                        Filters=[
                            {"Name": "association.subnet-id", "Values": [subnet_id]}
                        ]
                    )

                    for route_table in route_tables["RouteTables"]:
                        for route in route_table["Routes"]:
                            if route.get("GatewayId", "") == "igw-00000000":
                                # print(
                                #     f"RDS instance {instance_id} is running in a public subnet"
                                # )
                                rds_with_public_subnets = True
                                break
                        else:
                            continue
                        break
                else:
                    rds_with_public_subnets = False

                for vpcSecurityGroup in instance["VpcSecurityGroups"]:
                    vpcsecurity_group_id = vpcSecurityGroup["VpcSecurityGroupId"]
                    security_groups_info = ec2.describe_security_groups(
                        GroupIds=[vpcsecurity_group_id]
                    )

                    for security_group in security_groups_info["SecurityGroups"]:
                        # security_group_id = security_group['GroupId']

                        for rule in security_group["IpPermissions"]:
                            for ip_range in rule["IpRanges"]:
                                if ip_range["CidrIp"] == "0.0.0.0/0":
                                    rds_with_open_port = True
                                    break
                            else:
                                continue
                            break
                        else:
                            rds_with_open_port = False
                if rds_with_public_subnets and rds_with_open_port:
                    rds_with_public_subnets_with_open_ports.append(
                        {
                            "db_instance_identifier": db_instance_identifier,
                            "vpc_id": vpc_id,
                            "region": region,
                        }
                    )
        if rds_with_public_subnets_with_open_ports:
            self.data["AWS RDS with public subnets with open ports"] = {
                "count": len(rds_with_public_subnets_with_open_ports),
                "details": rds_with_public_subnets_with_open_ports,
                self.IDENTIFIER: "db_instance_identifier",
            }

        # ############################################
        # # Unencrypted AWS RDS instances (Security) #
        # ############################################
        unencrypted_rds_instances = []

        # iterate the instances and check for unencrypted ones
        for page in describe_db_instances_page_iterator:
            for instance in page["DBInstances"]:
                vpc_id = instance["DBSubnetGroup"]["VpcId"]
                if not instance["StorageEncrypted"]:
                    unencrypted_rds_instances.append(
                        {
                            "db_instance_identifier": instance["DBInstanceIdentifier"],
                            "vpc_id": vpc_id,
                            "region": region,
                        }
                    )

        if unencrypted_rds_instances:
            self.data["Unencrypted AWS RDS instances"] = {
                "count": len(unencrypted_rds_instances),
                "details": unencrypted_rds_instances,
                self.IDENTIFIER: "db_instance_identifier",
            }

        # #################################################
        # # Disabled Multi-AZ RDS instances (Reliability) #
        # #################################################
        disabled_multi_az_instances = []

        for page in describe_db_instances_page_iterator:
            for instance in page["DBInstances"]:
                vpc_id = instance["DBSubnetGroup"]["VpcId"]
                if not instance.get("MultiAZ"):
                    disabled_multi_az_instances.append(
                        {
                            "db_instance_identifier": instance["DBInstanceIdentifier"],
                            "vpc_id": vpc_id,
                            "region": region,
                        }
                    )

        if disabled_multi_az_instances:
            self.data["Disabled Multi-AZ RDS instances"] = {
                "count": len(disabled_multi_az_instances),
                "details": disabled_multi_az_instances,
                self.IDENTIFIER: "db_instance_identifier",
            }

        # ######################################################################
        # # Disabled point-in-time recovery for AWS RDS instance (Reliability) #
        # ######################################################################
        disabled_point_in_time_recovery_instances = []

        for page in describe_db_instances_page_iterator:
            for instance in page["DBInstances"]:
                vpc_id = instance["DBSubnetGroup"]["VpcId"]
                if instance["BackupRetentionPeriod"] == 0:
                    disabled_point_in_time_recovery_instances.append(
                        {
                            "db_instance_identifier": instance["DBInstanceIdentifier"],
                            "vpc_id": vpc_id,
                            "region": region,
                        }
                    )

        if disabled_point_in_time_recovery_instances:
            self.data["Disabled point-in-time recovery for AWS RDS instance"] = {
                "count": len(disabled_point_in_time_recovery_instances),
                "details": disabled_point_in_time_recovery_instances,
                self.IDENTIFIER: "db_instance_identifier",
            }

    def cloudtrail(self, region: str):
        """
        'policies' :describe_trails,
        """

        client = boto3.client("cloudtrail", region_name=region, **self.AWS_CREDS)

        trail_tags_cache = {}
        #######################################################
        # Disabled CloudTrail File Integrity Check (Security) #
        #######################################################
        disabled_trail_file_integrity_check = []

        describe_trails = client.describe_trails()
        for trail in describe_trails.get("trailList", []):
            trailArn = trail["TrailARN"]
            trail_tags = []
            if trailArn not in trail_tags_cache:
                try:
                    trail_tags = client.list_tags(
                        ResourceIdList=[
                            trailArn,
                        ]
                    )[
                        "ResourceTagList"
                    ][0]["TagsList"]
                except ClientError as e:
                    print(e)
                trail_tags_cache[trailArn] = trail_tags
            else:
                trail_tags = trail_tags_cache[trailArn]

            if not trail["LogFileValidationEnabled"]:
                disabled_trail_file_integrity_check.append(
                    {
                        "cloud_trail_name": trail["Name"],
                        "region_name": region,
                        "tags": trail_tags,
                    }
                )

        if disabled_trail_file_integrity_check:
            self.data["Disabled CloudTrail File Integrity Check"] = {
                "count": len(disabled_trail_file_integrity_check),
                "details": disabled_trail_file_integrity_check,
                self.IDENTIFIER: "cloud_trail_name",
            }

        ################################################
        # AWS CloudTrail event log disabled (Security) #
        ################################################
        cloudtrail_event_log_disabled = False

        if not describe_trails.get("trailList", []):
            cloudtrail_event_log_disabled = True

        if cloudtrail_event_log_disabled:
            self.data["AWS CloudTrail event log disabled"] = {
                "count": 1,
                "details": [{"status": True}],
                self.IDENTIFIER: "status",
                self.TYPE: "boolean",
                self.HRI_IF_STATUS_IS: True,
            }

    def guardduty(self):
        """
        'policies' :list_detectors, get_detector
        """

        client = boto3.client("guardduty", **self.AWS_CREDS)

        ##############################################
        # Disabled AWS GuardDuty Accounts (Security) #
        ##############################################
        disabled_guardduty = []

        is_disabled = True

        list_detectors_paginator = client.get_paginator("list_detectors")
        list_detectors_page_iterator = list_detectors_paginator.paginate()

        for page in list_detectors_page_iterator:
            for detector_id in page["DetectorIds"]:
                try:
                    status = client.get_detector(DetectorId=detector_id)["Status"]
                    if status == "ENABLED":
                        is_disabled = False
                        break

                except ClientError as e:
                    print(e)

        if is_disabled:
            disabled_guardduty.append({"region_name": client.meta.region_name})

        if disabled_guardduty:
            self.data["Disabled AWS GuardDuty accounts"] = {
                "count": len(disabled_guardduty),
                "details": disabled_guardduty,
                self.IDENTIFIER: "region_name",
            }

    def workspaces(self, region: str):
        """
        'policies' :describe_workspaces, describe_workspace_directories
        """

        client = boto3.client("workspaces", region_name=region, **self.AWS_CREDS)

        #########################################
        # Unattached Workspace Directory (Cost) #
        #########################################

        describe_workspaces_paginator = client.get_paginator("describe_workspaces")
        describe_workspaces_page_iterator = describe_workspaces_paginator.paginate()

        directories_id = []
        directories_tags_cache = {}

        for page in describe_workspaces_page_iterator:
            for workspace in page["Workspaces"]:
                if "DirectoryId" in workspace:
                    directories_id.append(workspace["DirectoryId"])

        describe_workspace_directories_paginator = client.get_paginator(
            "describe_workspace_directories"
        )
        describe_workspace_directories_page_iterator = (
            describe_workspace_directories_paginator.paginate()
        )

        unattached_directories = []

        for page in describe_workspace_directories_page_iterator:
            for directory in page["Directories"]:
                if directory["DirectoryId"] not in directories_id:
                    workspace_id = directory["DirectoryId"]
                    directories_tags = []
                    if workspace_id not in directories_tags_cache:
                        try:
                            directories_tags = client.describe_tags(
                                ResourceId=workspace_id
                            )["TagList"]
                        except ClientError as e:
                            print(e)
                        directories_tags_cache[workspace_id] = directories_tags
                    else:
                        directories_tags = directories_tags_cache[workspace_id]
                    unattached_directories.append(
                        {
                            "workspace_directory_id": directory["DirectoryId"],
                            "region_name": region,
                            "tags": directories_tags,
                        }
                    )

        if unattached_directories:
            self.add_to_hri_data(
                hri_data_key="Unattached Workspace Directory",
                table_row_list=unattached_directories,
                identifier="workspace_directory_id",
            )

    def elasticache(self, region: str):
        """
        'policies' :describe_replication_groups
        """

        client = boto3.client("elasticache", region_name=region, **self.AWS_CREDS)

        #########################################################
        # Disabled Multi-AZ Elasticache (Instances-Reliability) #
        #########################################################

        describe_replication_groups_paginator = client.get_paginator(
            "describe_replication_groups"
        )
        describe_replication_groups_page_iterator = (
            describe_replication_groups_paginator.paginate()
        )

        disabled_multi_az_elasticache_instances = []
        for page in describe_replication_groups_page_iterator:
            for replicationgroup in page["ReplicationGroups"]:
                if replicationgroup["MultiAZ"] == "disabled":
                    disabled_multi_az_elasticache_instances.append(
                        {"replication_group_id": replicationgroup["ReplicationGroupId"]}
                    )

        if disabled_multi_az_elasticache_instances:
            self.data["Disabled Multi-AZ Elasticache instances"] = {
                "count": len(disabled_multi_az_elasticache_instances),
                "details": disabled_multi_az_elasticache_instances,
                self.IDENTIFIER: "replication_group_id",
            }

    def dynamodb(self, region: str):
        """
        'policies' :list_tables, describe_continuous_backups, describe_scalable_targets-client(application-autoscaling)
        """

        client = boto3.client("dynamodb", region_name=region, **self.AWS_CREDS)

        ##############################################
        # Disabled AWS DynamoDB (Backup-Reliability) #
        ##############################################

        dynamodb_tags_cache = {}
        list_tables_paginator = client.get_paginator("list_tables")
        list_tables_page_iterator = list_tables_paginator.paginate()

        disabled_dynamoDB_backup = []

        for page in list_tables_page_iterator:
            table_names = page["TableNames"]
            for table_name in table_names:
                dynamodb_tags = []
                if table_name not in dynamodb_tags_cache:
                    try:
                        tableArn = client.describe_table(TableName=table_name)["Table"][
                            "TableArn"
                        ]
                        dynamodb_tags = client.list_tags_of_resource(
                            ResourceArn=tableArn
                        )["Tags"]
                    except ClientError as e:
                        print(e)
                    dynamodb_tags_cache[table_name] = dynamodb_tags
                else:
                    dynamodb_tags = dynamodb_tags_cache[table_name]
                try:
                    backup_response = client.describe_continuous_backups(
                        TableName=table_name
                    )
                    if "ContinuousBackupsDescription" in backup_response:
                        if (
                            backup_response["ContinuousBackupsDescription"][
                                "PointInTimeRecoveryDescription"
                            ]["PointInTimeRecoveryStatus"]
                            == "DISABLED"
                        ):
                            disabled_dynamoDB_backup.append(
                                {
                                    "table_name": table_name,
                                    "region_name": region,
                                    "tags": dynamodb_tags,
                                }
                            )

                    else:
                        print(f"Backups are not configured for table {table_name}")
                except ClientError as e:
                    print(e)

        if disabled_dynamoDB_backup:
            self.data["Disabled AWS DynamoDB backup"] = {
                "count": len(disabled_dynamoDB_backup),
                "details": disabled_dynamoDB_backup,
                self.IDENTIFIER: "table_name",
            }

        ###############################################
        # Disabled Autoscaling DynamoDB (Tables-Cost) #
        ###############################################

        disabled_autoscaling_dynamodb_tables = []

        for page in list_tables_page_iterator:
            for table_name in page["TableNames"]:
                application_autoscaling = boto3.client(
                    "application-autoscaling", **self.AWS_CREDS
                )
                response = application_autoscaling.describe_scalable_targets(
                    ServiceNamespace="dynamodb",
                    ResourceIds=[
                        "table/" + table_name,
                    ],
                )["ScalableTargets"]
                if not response:
                    dynamodb_tags = []
                    if table_name not in dynamodb_tags_cache:
                        try:
                            tableArn = client.describe_table(TableName=table_name)[
                                "Table"
                            ]["TableArn"]
                            dynamodb_tags = client.list_tags_of_resource(
                                ResourceArn=tableArn
                            )["Tags"]
                        except ClientError as e:
                            print(e)
                        dynamodb_tags_cache[table_name] = dynamodb_tags
                    else:
                        dynamodb_tags = dynamodb_tags_cache[table_name]
                    disabled_autoscaling_dynamodb_tables.append(
                        {
                            "table_name": table_name,
                            "region_name": region,
                            "tags": dynamodb_tags,
                        }
                    )

        if disabled_autoscaling_dynamodb_tables:
            self.add_to_hri_data(
                hri_data_key="Disabled autoscaling DynamoDB tables",
                table_row_list=disabled_autoscaling_dynamodb_tables,
                identifier="table_name",
            )

    def elbv2(self, region: str):
        """
        'policies' :describe_load_balancers, describe_target_groups, describe_target_health
        """

        client = boto3.client("elbv2", region_name=region, **self.AWS_CREDS)

        elb_tags_cache = {}
        ###################################
        # Unused AWS ELB (Resources-Cost) #
        ###################################

        describe_load_balancers_paginator = client.get_paginator(
            "describe_load_balancers"
        )
        describe_load_balancers_page_iterator = (
            describe_load_balancers_paginator.paginate()
        )

        unused_aws_elb = []
        for page in describe_load_balancers_page_iterator:
            if page:
                for load_balancer in page["LoadBalancers"]:
                    load_balancer_arn = load_balancer["LoadBalancerArn"]
                    elb_tags = []
                    if load_balancer_arn not in elb_tags_cache:
                        try:
                            elb_tags = client.describe_tags(
                                ResourceArns=[load_balancer_arn]
                            )["TagDescriptions"][0]["Tags"]
                        except ClientError as e:
                            continue
                        elb_tags_cache[load_balancer_arn] = elb_tags
                    else:
                        elb_tags = elb_tags_cache[load_balancer_arn]

                    target_groups = client.describe_target_groups(
                        LoadBalancerArn=load_balancer_arn
                    )["TargetGroups"]
                    for target_group in target_groups:
                        target_group_arn = target_group["TargetGroupArn"]

                        target_health = client.describe_target_health(
                            TargetGroupArn=target_group_arn
                        )["TargetHealthDescriptions"]

                        target_instances_unhealthy = None
                        for instance in target_health:
                            target_instances_unhealthy = True

                            if instance["TargetHealth"]["State"] == "healthy":
                                target_instances_unhealthy = False
                                break

                        if not target_health or target_instances_unhealthy:
                            unused_aws_elb.append(
                                {
                                    "load_balancer_name": load_balancer[
                                        "LoadBalancerName"
                                    ],
                                    "region_name": region,
                                    "tags": elb_tags,
                                }
                            )
        if unused_aws_elb:
            self.add_to_hri_data(
                hri_data_key="Unused AWS ELB resources",
                table_row_list=unused_aws_elb,
                identifier="load_balancer_name",
            )

    def inspector(self):
        """
        'policies' :list_assessment_runs
        """

        client = boto3.client("inspector", **self.AWS_CREDS)

        ################################################################
        # AWS Accounts with non-configured Amazon (Inspector-Security) #
        ################################################################

        non_configured_amazon_inspector = False
        try:
            list_assessment_runs_paginator = client.get_paginator(
                "list_assessment_runs"
            )
            list_assessment_runs_page_iterator = (
                list_assessment_runs_paginator.paginate()
            )
            for page in list_assessment_runs_page_iterator:
                if page:
                    if not page["assessmentRunArns"]:
                        non_configured_amazon_inspector = True

        except client.exceptions.NoSuchEntityException:
            print("Amazon Inspector is not configured in this account.")

        if non_configured_amazon_inspector:
            self.data["AWS Accounts with non configured Amazon Inspector"] = {
                "count": 1,
                "details": [{"status": True}],
                self.IDENTIFIER: "status",
                self.TYPE: "boolean",
                self.HRI_IF_STATUS_IS: True,
            }

    def waf(self):
        """
        'policies' :list_web_acls, list_resources_for_web_acl
        """

        client = boto3.client("wafv2", **self.AWS_CREDS)

        ################################################
        # AWS WAF without resource (Attached-Security) #
        ################################################

        waf_without_resource_attached = []
        try:
            list_web_acls = client.list_web_acls(Scope="REGIONAL")
            for web_acl in list_web_acls["WebACLs"]:
                web_acl_id = web_acl["Id"]
                web_acl_arn = web_acl["ARN"]
                try:
                    response = client.list_resources_for_web_acl(WebACLArn=web_acl_arn)[
                        "ResourceArns"
                    ]
                    if not response:
                        waf_without_resource_attached.append({"web_acl_id": web_acl_id})
                except ClientError as e:
                    print(e)
        except ClientError as e:
            print(e)

        if waf_without_resource_attached:
            self.data["AWS WAF without resource attached"] = {
                "count": len(waf_without_resource_attached),
                "details": waf_without_resource_attached,
                self.IDENTIFIER: "web_acl_id",
            }

    def cloudwatch(self, region: str):
        """
        'policies' :get_metric_statistics, describe_nat_gateways-ec2, describe_clusters-redshift, DescribeFileSystems
        """

        client = boto3.client("cloudwatch", region_name=region, **self.AWS_CREDS)

        ########################################
        # Low Traffic AWS EC2 (Instances-Cost) #
        ########################################

        low_traffic_ec2_instance = []
        cloudwatch_vpc_cache = {}
        cloudwatch_tags_cache = {}
        now = datetime.datetime.now()
        seven_days_ago = now - datetime.timedelta(days=7)

        ec2 = boto3.client("ec2", region_name=region, **self.AWS_CREDS)

        describe_instances_paginator = ec2.get_paginator("describe_instances")
        describe_instances_page_iterator = describe_instances_paginator.paginate()

        ec2_instance_ids = []
        for page in describe_instances_page_iterator:
            for reservation in page["Reservations"]:
                for instance in reservation["Instances"]:
                    if instance["State"]["Name"] == "running":
                        instance_id = instance["InstanceId"]
                        instance_tags = []
                        if "Tags" in instance:
                            instance_tags = instance["Tags"]
                        instance_vpc_id = instance["VpcId"]
                        if instance_id not in cloudwatch_vpc_cache:
                            cloudwatch_vpc_cache[instance_id] = instance_vpc_id
                        if instance_id not in cloudwatch_tags_cache:
                            cloudwatch_tags_cache[instance_id] = instance_tags
                        ec2_instance_ids.append(instance_id)

        for instance_id in ec2_instance_ids:
            cpu_response = client.get_metric_statistics(
                Namespace="AWS/EC2",
                MetricName="CPUUtilization",
                Dimensions=[
                    {"Name": "InstanceId", "Value": instance_id},
                ],
                StartTime=seven_days_ago,
                EndTime=now,
                Period=3600,
                Statistics=["Average"],
            )

            net_response = client.get_metric_statistics(
                Namespace="AWS/EC2",
                MetricName="NetworkIn",
                Dimensions=[
                    {"Name": "InstanceId", "Value": instance_id},
                ],
                StartTime=seven_days_ago,
                EndTime=now,
                Period=3600,
                Statistics=["Average"],
            )

            average_cpu_utilisation = True
            average_network_io = True
            for datapoint in cpu_response["Datapoints"]:
                if datapoint["Average"] >= 2:
                    average_cpu_utilisation = False
            for datapoint in net_response["Datapoints"]:
                if datapoint["Average"] >= 5 * 1024:
                    average_network_io = False

            if average_cpu_utilisation and average_network_io:
                low_traffic_ec2_instance.append(
                    {
                        "instance_id": instance_id,
                        "region_name": region,
                        "tags": cloudwatch_tags_cache[instance_id],
                        "vpc": cloudwatch_vpc_cache[instance_id],
                    }
                )

        if low_traffic_ec2_instance:
            self.add_to_hri_data(
                hri_data_key="Low traffic AWS EC2 instances",
                table_row_list=low_traffic_ec2_instance,
                identifier="instance_id",
            )

        ############################
        # RDS instance (Idle-Cost) #
        ############################

        rds = boto3.client("rds", region_name=region, **self.AWS_CREDS)

        describe_db_instances_paginator = rds.get_paginator("describe_db_instances")
        describe_db_instances_page_iterator = describe_db_instances_paginator.paginate()

        db_instance_identifiers = []
        for page in describe_db_instances_page_iterator:
            for db_instance in page["DBInstances"]:
                db_name = db_instance["DBInstanceIdentifier"]
                db_tags = []
                if "TagList" in db_instance:
                    db_tags = db_instance["TagList"]
                db_vpc_id = db_instance["DBSubnetGroup"]["VpcId"]
                if db_name not in cloudwatch_tags_cache:
                    cloudwatch_tags_cache[db_name] = db_tags
                if db_name not in cloudwatch_vpc_cache:
                    cloudwatch_vpc_cache[db_name] = db_vpc_id
                if db_instance["DBInstanceStatus"] == "available":
                    db_instance_identifiers.append(db_instance["DBInstanceIdentifier"])

        rds_instance_idle = []
        if db_instance_identifiers:
            for db_instance_identifier in db_instance_identifiers:
                db_response = client.get_metric_statistics(
                    Namespace="AWS/RDS",
                    MetricName="DatabaseConnections",
                    Dimensions=[
                        {
                            "Name": "DBInstanceIdentifier",
                            "Value": db_instance_identifier,
                        },
                    ],
                    StartTime=seven_days_ago.isoformat(),
                    EndTime=now.isoformat(),
                    Period=3600,
                    Statistics=["Average"],
                )

                average_cpu_utilisation = False
                for datapoint in db_response["Datapoints"]:
                    if datapoint["Average"] == 0:
                        average_cpu_utilisation = True
                    else:
                        average_cpu_utilisation = False
                        break
                if average_cpu_utilisation:
                    rds_instance_idle.append(
                        {
                            "db_instance": db_instance_identifier,
                            "region_name": region,
                            "tags": cloudwatch_tags_cache[db_instance_identifier],
                            "vpc": cloudwatch_vpc_cache[db_instance_identifier],
                        }
                    )

        if rds_instance_idle:
            self.add_to_hri_data(
                hri_data_key="RDS instance idle",
                table_row_list=rds_instance_idle,
                identifier="db_instance",
            )

        ###################################
        # Review RDS instance (Size-Cost) #
        ###################################

        under_utilized_rds_instance = []
        for db_instance_identifier in db_instance_identifiers:
            rds_cpu_response = client.get_metric_statistics(
                Namespace="AWS/RDS",
                MetricName="CPUUtilization",
                Dimensions=[
                    {"Name": "DBInstanceIdentifier", "Value": db_instance_identifier},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Average"],
            )
            rds_readiops_response = client.get_metric_statistics(
                Namespace="AWS/RDS",
                MetricName="ReadIOPS",
                Dimensions=[
                    {"Name": "DBInstanceIdentifier", "Value": db_instance_identifier},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Sum"],
            )
            rds_writeiops_response = client.get_metric_statistics(
                Namespace="AWS/RDS",
                MetricName="WriteIOPS",
                Dimensions=[
                    {"Name": "DBInstanceIdentifier", "Value": db_instance_identifier},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Sum"],
            )

            rds_cpu_utilization = True
            for cpu in rds_cpu_response["Datapoints"]:
                average = cpu["Average"]
                if average >= 30:
                    rds_cpu_utilization = False
                    break

            rds_total_read_iops = 0
            rds_total_write_iops = 0
            for read_iop in rds_readiops_response["Datapoints"]:
                rds_total_read_iops = +read_iop["Sum"]
            for write_iop in rds_writeiops_response["Datapoints"]:
                rds_total_write_iops = +write_iop["Sum"]

            if (
                rds_cpu_utilization
                and rds_total_read_iops < 100
                and rds_total_write_iops < 100
            ):
                under_utilized_rds_instance.append(
                    {
                        "db_instance_identifier": db_instance_identifier,
                        "region_name": region,
                        "tags": cloudwatch_tags_cache[db_instance_identifier],
                        "vpc": cloudwatch_vpc_cache[db_instance_identifier],
                    }
                )

        if under_utilized_rds_instance:
            self.add_to_hri_data(
                hri_data_key="Review RDS instance size",
                table_row_list=under_utilized_rds_instance,
                identifier="db_instance_identifier",
            )

        ##########################################################
        # Underutilized (<30% read/write) DynamoDB (Tables-Cost) #
        ##########################################################

        dynamodb = boto3.client("dynamodb", region_name=region, **self.AWS_CREDS)

        list_tables_paginator = dynamodb.get_paginator("list_tables")
        list_tables_page_iterator = list_tables_paginator.paginate()

        underutilized_tables = []
        table_names = []
        dynamodb_tags = []
        for page in list_tables_page_iterator:
            table_names.extend(page["TableNames"])

        for table in table_names:
            table_name = table
            if table_name not in cloudwatch_tags_cache:
                try:
                    tableArn = dynamodb.describe_table(TableName=table_name)["Table"][
                        "TableArn"
                    ]
                    dynamodb_tags = dynamodb.list_tags_of_resource(
                        ResourceArn=tableArn
                    )["Tags"]
                except ClientError as e:
                    print(e)
                cloudwatch_tags_cache[table_name] = dynamodb_tags
            else:
                dynamodb_tags = cloudwatch_tags_cache[table_name]

            provisioned_read_response = client.get_metric_statistics(
                Namespace="AWS/DynamoDB",
                MetricName="ProvisionedReadCapacityUnits",
                Dimensions=[
                    {"Name": "TableName", "Value": table_name},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Average"],
            )
            consumed_read_response = client.get_metric_statistics(
                Namespace="AWS/DynamoDB",
                MetricName="ConsumedReadCapacityUnits",
                Dimensions=[
                    {"Name": "TableName", "Value": table_name},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Average"],
            )
            provisioned_write_response = client.get_metric_statistics(
                Namespace="AWS/DynamoDB",
                MetricName="ProvisionedWriteCapacityUnits ",
                Dimensions=[
                    {"Name": "TableName", "Value": table_name},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Average"],
            )
            consumed_write_response = client.get_metric_statistics(
                Namespace="AWS/DynamoDB",
                MetricName="ConsumedWriteCapacityUnits ",
                Dimensions=[
                    {"Name": "TableName", "Value": table_name},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Average"],
            )

            total_provisioned_read_units = sum(
                [d["Average"] for d in provisioned_read_response["Datapoints"]]
            )
            total_consumed_read_units = sum(
                [d["Average"] for d in consumed_read_response["Datapoints"]]
            )
            total_provisioned_write_units = sum(
                [d["Average"] for d in provisioned_write_response["Datapoints"]]
            )
            total_consumed_write_units = sum(
                [d["Average"] for d in consumed_write_response["Datapoints"]]
            )

            if total_consumed_read_units <= (
                (70 / 100) * total_provisioned_read_units
            ) and total_consumed_write_units <= (
                (70 / 100) * total_provisioned_write_units
            ):
                underutilized_tables.append(
                    {
                        "table_name": table_name,
                        "region_name": region,
                        "tags": dynamodb_tags,
                    }
                )

        if underutilized_tables:
            self.add_to_hri_data(
                hri_data_key="Underutilized (<30% read/write) DynamoDB tables",
                table_row_list=underutilized_tables,
                identifier="table_name",
            )

        ##########################################
        # Unused AWS NAT (Resources-Performance) #
        ##########################################

        nat_gateways = ec2.describe_nat_gateways()["NatGateways"]

        unused_nat = []
        if nat_gateways:
            for nat in nat_gateways:
                nat_gate_way_id = nat["NatGatewayId"]
                nat_gate_way_state = nat["State"]
                nat_gate_way_tags = []
                if "Tags" in nat:
                    nat_gate_way_tags = nat["Tags"]
                nat_gate_way_vpc = nat["VpcId"]
                if nat_gate_way_state == "available":
                    nat_response = client.get_metric_statistics(
                        Namespace="AWS/NATGateway",
                        MetricName="BytesOutToDestination",
                        Dimensions=[
                            {"Name": "InstanceId", "Value": nat_gate_way_id},
                        ],
                        StartTime=seven_days_ago.isoformat(),
                        EndTime=now.isoformat(),
                        Period=3600,
                        Statistics=["Average"],
                    )

                    bytes_out = nat_response["Datapoints"]
                    if not bytes_out:
                        unused_nat.append(
                            {
                                "nat_id": nat_gate_way_id,
                                "region_name": region,
                                "tags": nat_gate_way_tags,
                                "vpc": nat_gate_way_vpc,
                            }
                        )

        if unused_nat:
            self.add_to_hri_data(
                hri_data_key="Unused AWS NAT resources",
                table_row_list=unused_nat,
                identifier="nat_id",
            )

        ######################################################
        # Underutilized Redshift Cluster (Nodes-Performance) #
        ######################################################

        redshift = boto3.client("redshift", region_name=region, **self.AWS_CREDS)

        thirty_days_ago = now - datetime.timedelta(days=30)

        describe_clusters_paginator = redshift.get_paginator("describe_clusters")
        describe_clusters_page_iterator = describe_clusters_paginator.paginate()

        redshift_cluster_identifiers = []
        for page in describe_clusters_page_iterator:
            for cluster in page["Clusters"]:
                cluster_identifier = cluster["ClusterIdentifier"]
                redshift_cluster_identifiers.append(cluster_identifier)

        under_utilized_redshift = []
        for cluster_id in redshift_cluster_identifiers:
            cluster_identifier = cluster_id
            cpu_response = client.get_metric_statistics(
                Namespace="AWS/Redshift",
                MetricName="CPUUtilization ",
                Dimensions=[
                    {"Name": "ClusterIdentifier", "Value": cluster_identifier},
                ],
                StartTime=thirty_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Average"],
            )
            readiops_response = client.get_metric_statistics(
                Namespace="AWS/Redshift",
                MetricName="ReadIOPS",
                Dimensions=[
                    {"Name": "ClusterIdentifier", "Value": cluster_identifier},
                ],
                StartTime=thirty_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Sum"],
            )
            writeiops_response = client.get_metric_statistics(
                Namespace="AWS/Redshift",
                MetricName="WriteIOPS",
                Dimensions=[
                    {"Name": "ClusterIdentifier", "Value": cluster_identifier},
                ],
                StartTime=thirty_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Sum"],
            )

            utilization = True
            total_read_iops = 0
            total_write_iops = 0
            for cpu in cpu_response["Datapoints"]:
                average = cpu["Average"]
                if average > 0.6:
                    utilization = False
                    break
            for read_iop in readiops_response["Datapoints"]:
                total_read_iops = +read_iop["Sum"]
            for write_iop in writeiops_response["Datapoints"]:
                total_write_iops = +write_iop["Sum"]

            if utilization and total_read_iops < 100 and total_write_iops < 100:
                under_utilized_redshift.append({"cluster_name": cluster_identifier})

        if under_utilized_redshift:
            self.data["Underutilized Redshift Cluster Nodes"] = {
                "count": len(under_utilized_redshift),
                "details": under_utilized_redshift,
                self.IDENTIFIER: "cluster_identifier",
            }

        #################################################
        # Underutilized AWS EBS provisioned (IOPS-Cost) #
        #################################################

        underutilized_provisioned_iops = []

        describe_volumes_paginator = ec2.get_paginator("describe_volumes")
        describe_volumes_page_iterator = describe_volumes_paginator.paginate()

        ec2_volume_ids = []
        iops = {}
        for page in describe_volumes_page_iterator:
            for volume in page["Volumes"]:
                if volume["State"] == "in-use" or volume["State"] == "available":
                    volume_id = volume["VolumeId"]
                    volume_tags = []
                    if "Tags" in volume:
                        volume_tags = volume["Tags"]
                    cloudwatch_tags_cache[volume_id] = volume_tags
                    ec2_volume_ids.append(volume_id)
                    iops[volume_id] = volume["Iops"]

        if ec2_volume_ids:
            for volume_id in ec2_volume_ids:
                write_response = client.get_metric_statistics(
                    Namespace="AWS/EBS",
                    MetricName="VolumeWriteOps",
                    Dimensions=[
                        {"Name": "VolumeId", "Value": volume_id},
                    ],
                    StartTime=seven_days_ago.isoformat(),
                    EndTime=now.isoformat(),
                    Period=3600,
                    Statistics=["Average"],
                )
                read_response = client.get_metric_statistics(
                    Namespace="AWS/EBS",
                    MetricName="VolumeReadOps",
                    Dimensions=[
                        {"Name": "VolumeId", "Value": volume_id},
                    ],
                    StartTime=seven_days_ago.isoformat(),
                    EndTime=now.isoformat(),
                    Period=3600,
                    Statistics=["Average"],
                )
                volume_write_iops = sum(
                    [d["Average"] for d in write_response["Datapoints"]]
                )
                volume_read_iops = sum(
                    [d["Average"] for d in read_response["Datapoints"]]
                )
                provisioned_iops = iops[volume_id]
                utilization = (volume_write_iops + volume_read_iops) / provisioned_iops
                if utilization < 0.7:
                    underutilized_provisioned_iops.append(
                        {
                            "volume_id": volume_id,
                            "region_name": region,
                            "tags": cloudwatch_tags_cache[volume_id],
                        }
                    )

        if underutilized_provisioned_iops:
            self.add_to_hri_data(
                hri_data_key="Underutilized AWS EBS provisioned IOPS",
                table_row_list=underutilized_provisioned_iops,
                identifier="volume_id",
            )

        #################################################
        # Underutilized AWS RDS provisioned (IOPS-Cost) #
        #################################################

        underutilized_rds_provisioned_iops = []

        for db_instance in db_instance_identifiers:
            db_instance_identifier = db_instance
            rds_cpu_response = client.get_metric_statistics(
                Namespace="AWS/RDS",
                MetricName="CPUUtilization",
                Dimensions=[
                    {"Name": "DBInstanceIdentifier", "Value": db_instance_identifier},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Average"],
            )
            rds_readiops_response = client.get_metric_statistics(
                Namespace="AWS/RDS",
                MetricName="ReadIOPS",
                Dimensions=[
                    {"Name": "DBInstanceIdentifier", "Value": db_instance_identifier},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Sum"],
            )
            rds_writeiops_response = client.get_metric_statistics(
                Namespace="AWS/RDS",
                MetricName="WriteIOPS",
                Dimensions=[
                    {"Name": "DBInstanceIdentifier", "Value": db_instance_identifier},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Sum"],
            )
            rds_cpu_utilization = sum(
                [d["Average"] for d in rds_cpu_response["Datapoints"]]
            ) / len(rds_cpu_response["Datapoints"])
            rds_write_iops = len(rds_writeiops_response["Datapoints"])
            rds_read_iops = len(rds_readiops_response["Datapoints"])

            if (
                rds_cpu_utilization < 30
                and rds_write_iops < 100
                and rds_read_iops < 100
            ):
                underutilized_rds_provisioned_iops.append(
                    {
                        "db_instance_identifier": db_instance_identifier,
                        "region_name": region,
                        "tags": cloudwatch_tags_cache[db_instance_identifier],
                        "vpc": cloudwatch_vpc_cache[db_instance_identifier],
                    }
                )

        if underutilized_rds_provisioned_iops:
            self.add_to_hri_data(
                hri_data_key="Underutilized AWS RDS provisioned IOPS",
                table_row_list=underutilized_rds_provisioned_iops,
                identifier="db_instance_identifier",
            )

        ###################################
        # Review EC2 instance (Size-Cost) #
        ###################################

        ec2_instance_rightsizing = []
        read_iops_instance = {}
        write_iops_instance = {}

        for instance_id in ec2_instance_ids:
            volumes = ec2.describe_volumes(
                Filters=[{"Name": "attachment.instance-id", "Values": [instance_id]}]
            )
            for volume in volumes["Volumes"]:
                volume_id = volume["VolumeId"]
                volume_readiops_response = client.get_metric_statistics(
                    Namespace="AWS/EBS",
                    MetricName="VolumeReadOps",
                    Dimensions=[
                        {"Name": "VolumeId", "Value": volume_id},
                    ],
                    StartTime=seven_days_ago.isoformat(),
                    EndTime=now.isoformat(),
                    Period=3600,
                    Statistics=["Sum"],
                )
                volume_writeiops_response = client.get_metric_statistics(
                    Namespace="AWS/EBS",
                    MetricName="VolumeWriteOps",
                    Dimensions=[
                        {"Name": "VolumeId", "Value": volume_id},
                    ],
                    StartTime=seven_days_ago.isoformat(),
                    EndTime=now.isoformat(),
                    Period=3600,
                    Statistics=["Sum"],
                )

                read_iops_instance[instance_id] = len(
                    volume_readiops_response["Datapoints"]
                )
                write_iops_instance[instance_id] = len(
                    volume_writeiops_response["Datapoints"]
                )

            ec2_cpu_response = client.get_metric_statistics(
                Namespace="AWS/EC2",
                MetricName="CPUUtilization",
                Dimensions=[{"Name": "InstanceId", "Value": instance_id}],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Average"],
            )

            average_ec2_cpu_utilization = 0
            if ec2_cpu_response["Datapoints"]:
                average_ec2_cpu_utilization = sum(
                    [d["Average"] for d in ec2_cpu_response["Datapoints"]]
                ) / len(ec2_cpu_response["Datapoints"])

            if (
                average_ec2_cpu_utilization < 30
                and read_iops_instance[instance_id] < 100
                and write_iops_instance[instance_id] < 100
            ):
                ec2_instance_rightsizing.append(
                    {
                        "instance_id": instance_id,
                        "region_name": region,
                        "tags": cloudwatch_tags_cache[instance_id],
                        "vpc": cloudwatch_vpc_cache[instance_id],
                    }
                )

        if ec2_instance_rightsizing:
            self.data["Review EC2 instance size"] = {
                "count": len(ec2_instance_rightsizing),
                "details": ec2_instance_rightsizing,
                self.IDENTIFIER: "instance_id",
            }

        ##################################
        # Review S3 storage (Class-Cost) #
        ##################################

        s3 = boto3.client("s3", region_name=region, **self.AWS_CREDS)

        s3_buckets = s3.list_buckets()
        s3_recommendation_rightsizing = []
        s3_buckets_tags = []
        for bucket in s3_buckets["Buckets"]:
            bucket_name = bucket["Name"]
            if bucket_name not in cloudwatch_tags_cache:
                try:
                    s3_buckets_tags = s3.get_bucket_tagging(Bucket=bucket_name)[
                        "TagSet"
                    ]
                except ClientError as e:
                    continue
                cloudwatch_tags_cache[bucket_name] = s3_buckets_tags
            else:
                s3_buckets_tags = cloudwatch_tags_cache[bucket_name]
            s3_objects_count = client.get_metric_statistics(
                Namespace="AWS/S3",
                MetricName="NumberOfObjects",
                Dimensions=[
                    {"Name": "BucketName", "Value": bucket_name},
                    {"Name": "StorageType", "Value": "AllStorageTypes"},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Sum"],
                Unit="Count",
            )
            s3_size = client.get_metric_statistics(
                Namespace="AWS/S3",
                MetricName="BucketSizeBytes",
                Dimensions=[
                    {"Name": "BucketName", "Value": bucket_name},
                    {"Name": "StorageType", "Value": "StandardStorage"},
                ],
                StartTime=seven_days_ago.isoformat(),
                EndTime=now.isoformat(),
                Period=3600,
                Statistics=["Average"],
                Unit="Bytes",
            )

            total_max_object_count = 0
            if len(s3_objects_count["Datapoints"]) > 1:
                total_max_object_count = max(
                    [d["Sum"] for d in s3_objects_count["Datapoints"]]
                )
            total_max_size = 0
            if len(s3_size["Datapoints"]) > 1:
                total_max_size = (
                    max([d["Average"] for d in s3_size["Datapoints"]]) / 1e6
                )

            if total_max_object_count < 1 and total_max_size < 0.1:
                s3_recommendation_rightsizing.append(
                    {
                        "bucket_name": bucket_name,
                        "region_name": region,
                        "tags": s3_buckets_tags,
                    }
                )

        if s3_recommendation_rightsizing:
            self.add_to_hri_data(
                hri_data_key="Review S3 storage class",
                table_row_list=s3_recommendation_rightsizing,
                identifier="bucket_name",
            )

        #################################################
        # Underutilized AWS CloudWatch Log (Group-Cost) #
        #################################################

        cloudwatch_logs = boto3.client("logs", region_name=region, **self.AWS_CREDS)
        underutilized_cloudWatch_log_group = []

        try:
            describe_log_groups_paginator = cloudwatch_logs.get_paginator(
                "describe_log_groups"
            )
            describe_log_groups_page_iterator = describe_log_groups_paginator.paginate()

            log_group_names = []
            logs_tags = []
            for page in describe_log_groups_page_iterator:
                for log_group in page["logGroups"]:
                    log_group_name = log_group["logGroupName"]
                    if log_group_name not in cloudwatch_tags_cache:
                        try:
                            logs_tags = cloudwatch_logs.list_tags_log_group(
                                logGroupName=log_group_name
                            )["tags"]
                        except ClientError as e:
                            print(e)
                        cloudwatch_tags_cache[log_group_name] = logs_tags
                    else:
                        logs_tags = cloudwatch_tags_cache[log_group_name]
                    log_group_names.append(log_group_name)

            if log_group_names:
                for log_group in log_group_names:
                    log_group_name = log_group
                    log_response = client.get_metric_statistics(
                        Namespace="AWS/Logs",
                        MetricName="IncomingBytes",
                        Dimensions=[
                            {"Name": "LogGroupName", "Value": log_group_name},
                        ],
                        StartTime=seven_days_ago.isoformat(),
                        EndTime=now.isoformat(),
                        Period=3600,
                        Statistics=["Sum"],
                        Unit="Bytes",
                    )
                    total_bytes = sum([d["Sum"] for d in log_response["Datapoints"]])
                    if total_bytes == 0:
                        underutilized_cloudWatch_log_group.append(
                            {
                                "log_group_name": log_group_name,
                                "region_name": region,
                                "tags": cloudwatch_tags_cache[log_group_name],
                            }
                        )
        except ClientError as e:
            print(e)

        if underutilized_cloudWatch_log_group:
            self.add_to_hri_data(
                hri_data_key="Underutilized AWS CloudWatch Log Group",
                table_row_list=underutilized_cloudWatch_log_group,
                identifier="log_group_name",
            )

        #############################################
        # Infrequently accessed EFS (Resource-Cost) #
        #############################################

        efs = boto3.client("efs", region_name=region, **self.AWS_CREDS)

        describe_file_systems_paginator = efs.get_paginator("describe_file_systems")
        describe_file_systems_page_iterator = describe_file_systems_paginator.paginate()

        infrequently_accessed_efs_resource = []
        file_system_ids = []
        file_system_tags = []
        for page in describe_file_systems_page_iterator:
            for file_system in page["FileSystems"]:
                file_system_id = file_system["FileSystemId"]
                if "Tags" in file_system:
                    file_system_tags = file_system["Tags"]
                    cloudwatch_tags_cache[file_system_id] = file_system_tags
                file_system_ids.append(file_system_id)

        if file_system_ids:
            for file_system in file_system_ids:
                file_system_id = file_system
                data_read_response = client.get_metric_statistics(
                    Namespace="AWS/EFS",
                    MetricName="DataReadIOBytes",
                    Dimensions=[
                        {"Name": "FileSystemId", "Value": file_system_id},
                    ],
                    StartTime=thirty_days_ago.isoformat(),
                    EndTime=now.isoformat(),
                    Period=3600,
                    Statistics=["Average"],
                )
                data_write_response = client.get_metric_statistics(
                    Namespace="AWS/EFS",
                    MetricName="DataWriteIOBytes",
                    Dimensions=[
                        {"Name": "FileSystemId", "Value": file_system_id},
                    ],
                    StartTime=thirty_days_ago.isoformat(),
                    EndTime=now.isoformat(),
                    Period=3600,
                    Statistics=["Average"],
                )

                # Define the threshold for infrequent access
                data_read_threshold = 10000000  # 10 MB/hour
                data_write_threshold = 10000000  # 10 MB/hour

                average_read_bytes = 0
                average_write_bytes = 0
                if data_read_response["Datapoints"]:
                    average_read_bytes = sum(
                        [d["Average"] for d in data_read_response["Datapoints"]]
                    ) / len(data_read_response["Datapoints"])

                if data_write_response["Datapoints"]:
                    average_write_bytes = sum(
                        [d["Average"] for d in data_write_response["Datapoints"]]
                    ) / len(data_write_response["Datapoints"])

                if (
                    average_read_bytes < data_read_threshold
                    and average_write_bytes < data_write_threshold
                ):
                    infrequently_accessed_efs_resource.append(
                        {
                            "file_system_id": file_system_id,
                            "region_name": region,
                            "tags": cloudwatch_tags_cache[file_system_id],
                        }
                    )

        if infrequently_accessed_efs_resource:
            self.data["Infrequently accessed EFS resource"] = {
                "count": len(infrequently_accessed_efs_resource),
                "details": infrequently_accessed_efs_resource,
                self.IDENTIFIER: "file_system_id",
            }

        ###############################################
        # Underutilized (<10%) AWS ECS (Cluster-Cost) #
        ###############################################

        ecs = boto3.client("ecs", region_name=region, **self.AWS_CREDS)
        ecs_clusters = ecs.list_clusters()

        underutilized_aws_ecs_cluster = []
        if ecs_clusters["clusterArns"]:
            for cluster in ecs_clusters["clusterArns"]:
                current_cluster = ecs.describe_clusters(clusters=[cluster])
                cluster_name = current_cluster["clusters"][0]["clusterName"]
                cloudwatch_tags_cache[cluster_name] = current_cluster["clusters"][0][
                    "tags"
                ]
                cpu_response = client.get_metric_statistics(
                    Namespace="AWS/ECS",
                    MetricName="CPUUtilization",
                    Dimensions=[
                        {"Name": "ClusterName", "Value": cluster_name},
                    ],
                    StartTime=seven_days_ago.isoformat(),
                    EndTime=now.isoformat(),
                    Period=3600,
                    Statistics=["Average"],
                    Unit="Percent",
                )

                cpu_utilization = 0
                if cpu_response["Datapoints"]:
                    cpu_utilization = sum(
                        [d["Average"] for d in cpu_response["Datapoints"]]
                    ) / len(cpu_response["Datapoints"])

                if cpu_utilization < 10:
                    underutilized_aws_ecs_cluster.append(
                        {
                            "ecs_cluster_name": cluster_name,
                            "region_name": region,
                            "tags": cloudwatch_tags_cache[cluster_name],
                        }
                    )

        if underutilized_aws_ecs_cluster:
            self.data["Underutilized (<10%) AWS ECS cluster"] = {
                "count": len(underutilized_aws_ecs_cluster),
                "details": underutilized_aws_ecs_cluster,
                self.IDENTIFIER: "ecs_cluster_name",
            }

        ###########################################
        # Find Underutilized ec2 (Instances-Cost) #
        ###########################################

        client_ec2 = boto3.client("ec2", region_name=region, **self.AWS_CREDS)
        list_ec2_paginator = client_ec2.get_paginator("describe_instances")
        list_ec2_page_iterator = list_ec2_paginator.paginate()

        underutilized_ec2_instances = []
        for page in list_ec2_page_iterator:
            for reserve_ec2 in page["Reservations"]:
                instance = reserve_ec2["Instances"][0]
                instance_id = instance["InstanceId"]
                instance_tags = []
                if "Tags" in instance:
                    instance_tags = instance["Tags"]
                instance_vpc_id = instance["VpcId"]
                if instance_id not in cloudwatch_vpc_cache:
                    cloudwatch_vpc_cache[instance_id] = instance_vpc_id
                if instance_id not in cloudwatch_tags_cache:
                    cloudwatch_tags_cache[instance_id] = instance_tags
                CPUUtilization = client.get_metric_statistics(
                    Namespace="AWS/EC2",
                    MetricName="CPUUtilization",
                    Dimensions=[
                        {"Name": "InstanceId", "Value": instance["InstanceId"]}
                    ],
                    StartTime=seven_days_ago.isoformat(),
                    EndTime=now.isoformat(),
                    Period=3600,
                    Statistics=["Average"],
                )
                disk_read_response = client.get_metric_statistics(
                    Namespace="AWS/EC2",
                    MetricName="DiskReadOps",
                    Dimensions=[
                        {"Name": "InstanceId", "Value": instance["InstanceId"]},
                    ],
                    StartTime=seven_days_ago.isoformat(),
                    EndTime=now.isoformat(),
                    Period=3600,
                    Statistics=["Average"],
                )
                disk_write_response = client.get_metric_statistics(
                    Namespace="AWS/EC2",
                    MetricName="DiskWriteOps",
                    Dimensions=[
                        {"Name": "InstanceId", "Value": instance["InstanceId"]},
                    ],
                    StartTime=seven_days_ago.isoformat(),
                    EndTime=now.isoformat(),
                    Period=3600,
                    Statistics=["Average"],
                )
                cpu_utilization_ec2 = 0
                if CPUUtilization["Datapoints"]:
                    cpu_utilization_ec2 = sum(
                        [d["Average"] for d in CPUUtilization["Datapoints"]]
                    ) / len(CPUUtilization["Datapoints"])
                memory_utilization_ec2 = 0
                if (
                    disk_read_response["Datapoints"]
                    or disk_write_response["Datapoints"]
                ):
                    memory_utilization_ec2 = sum(
                        [
                            d["Average"]
                            for d in disk_read_response["Datapoints"]
                            + disk_write_response["Datapoints"]
                        ]
                    ) / len(
                        disk_read_response["Datapoints"]
                        + disk_write_response["Datapoints"]
                    )

                if (
                    cpu_utilization_ec2 < 30
                    and memory_utilization_ec2 < 30
                    and instance["State"]["Name"] == "running"
                ):
                    underutilized_ec2_instances.append(
                        {
                            "instance_id": instance["InstanceId"],
                            "region_name": region,
                            "tags": cloudwatch_tags_cache[instance["InstanceId"]],
                            "vpc": cloudwatch_vpc_cache[instance["InstanceId"]],
                        }
                    )

        if underutilized_ec2_instances:
            self.data["Underutilized ec2 instances"] = {
                "count": len(underutilized_ec2_instances),
                "details": underutilized_ec2_instances,
                self.IDENTIFIER: "instance_id",
            }

    def support(self, region: str):
        """
        'policies' :DescribeSeverityLevels
        """

        client = boto3.client("support", region_name=region, **self.AWS_CREDS)

        vpc = "-"
        Tags = "-"
        ################################################
        # Disabled AWS Enterprise (Support-Operations) #
        ################################################

        disabled_aws_enterprise_support = True
        try:
            response = client.describe_severity_levels()
            print(response)
            for severity_level in response["severityLevels"]:
                if (
                    severity_level["code"] == "critical"
                    and severity_level["name"] == "critical"
                ):
                    disabled_aws_enterprise_support = False
                    break
        except ClientError as e:
            if (
                e.response.get("Error", {}).get("Code", "")
                == "SubscriptionRequiredException"
            ):
                disabled_aws_enterprise_support = True

        if disabled_aws_enterprise_support:
            self.data["Disabled AWS Enterprise support"] = {
                "count": 1,
                "details": [
                    {"status": True, "region": region, "vpc": vpc, "Tag": Tags}
                ],
                self.IDENTIFIER: "status",
                self.TYPE: "boolean",
                self.HRI_IF_STATUS_IS: True,
            }

    def aws_lambda(self, region: str):
        """
        'policies' :ListFunctions, GetFunction, GetPolicy
        """

        client = boto3.client("lambda", region_name=region, **self.AWS_CREDS)

        vpc = "-"
        ##########################################################################
        # Lambda environment variables without (CMK encryption enabled-security) #
        ##########################################################################

        list_functions_paginator = client.get_paginator("list_functions")
        list_functions_page_iterator = list_functions_paginator.paginate()

        function_names = []
        for page in list_functions_page_iterator:
            if page:
                for function in page["Functions"]:
                    function_name = function["FunctionName"]
                    function_names.append(function_name)

        lambda_variables_without_cmk_encryption_enabled = []
        for function_name in function_names:
            try:
                response = client.get_function(FunctionName=function_name)
                function_arn = response["Configuration"]["FunctionArn"]
                kms_key_arn = response["Configuration"]
                if "KMSKeyArn" in kms_key_arn:
                    if kms_key_arn["KMSKeyArn"] is None:
                        lambda_variables_without_cmk_encryption_enabled.append(
                            {
                                "lambda_function_name": function_name,
                                "lambda_function_region": region,
                                "Vpc_id": vpc,
                            }
                        )
            except ClientError as e:
                print(e)

        if lambda_variables_without_cmk_encryption_enabled:
            self.data["Lambda environment variables without CMK encryption enabled"] = {
                "count": len(lambda_variables_without_cmk_encryption_enabled),
                "details": lambda_variables_without_cmk_encryption_enabled,
                self.IDENTIFIER: "lambda_function_name",
            }

        ##############################################
        # Lambda function without (Trigger-Security) #
        ##############################################

        lambda_functions_without_trigger = []
        for function_name in function_names:
            function_policy = ""
            try:
                function_policy = client.get_policy(FunctionName=function_name)
                function_policy = function_policy["Policy"]

                response = client.get_function(FunctionName=function_name)
                function_arn = response["Configuration"]["FunctionArn"]

            except ClientError as e:
                print(e)

            if "Action" not in function_policy:
                lambda_functions_without_trigger.append(
                    {"lambda_function_name": function_name, "function_region": region}
                )

        if lambda_functions_without_trigger:
            self.data["Lambda function without trigger"] = {
                "count": len(lambda_functions_without_trigger),
                "details": lambda_functions_without_trigger,
                self.IDENTIFIER: "lambda_function_name",
            }

        # #######################################################
        # # Public Lambda function without (Exception-Security) #
        # #######################################################

        public_lambda = []
        for function_name in function_names:
            try:
                policy = client.get_policy(FunctionName=function_name)
                policy_document = policy["Policy"]
                policy_dict = json.loads(policy_document)

                response = client.get_function(FunctionName=function_name)
                function_arn = response["Configuration"]["FunctionArn"]

                for statement in policy_dict["Statement"]:
                    if "Principal" in statement and statement["Principal"] == "*":
                        public_lambda.append(
                            {
                                "lambda_function_name": function_name,
                                "function_region": region,
                            }
                        )

            except ClientError as e:
                print(e)

            if public_lambda:
                self.data["Public Lambda function without exception"] = {
                    "count": len(public_lambda),
                    "details": public_lambda,
                    self.IDENTIFIER: "lambda_function_name",
                }

    def get_hri_final_data(self,region):
        new_data = {"cloud_type": "AWS", "hri_data": dict()}

        # Create a list to hold the threads
        # threads = []

        # # Create a thread for each region and append it to the threads list
        # for region in regions:
        #     thread = threading.Thread(target=self.s3, args=(region,))
        #     thread.start()
        #     threads.append(thread)

        # num_threads = threading.active_count()
        # print(f"There are currently {num_threads} threads running.")
        # # Wait for all threads to finish before continuing
        # for thread in threads:
        #     thread.join()

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit a task to the thread pool for each region
            futures = [executor.submit(self.s3, region) for region in region_names]

            # Wait for all tasks to complete
            concurrent.futures.wait(futures)

        self.s3(region)
        self.elbv2(region)
        self.support(region)
        self.cloudwatch(region)
        self.iam(region)
        self.config(region)
        self.cloudtrail(region)
        self.ec2(region)
        self.elasticache(region)
        self.dynamodb(region)
        self.rds(region)
        self.workspaces(region)
        self.aws_lambda(region)

        all_hris_data = self.data
        # print(all_hris_data)
        # all_hris_data = get_hri_data_with_cost(
        #     all_hris_data,
        #     self.hri_resource_id_dict,
        #     bucket="cru-try",
        #     prefix="temp-cur-try/samarth_temp_try1",
        # )
        # for hri_name, hri_data in all_hris_data.items():
        #     key = "aws_" + hri_name
        #     reverse_key = self.hri_reverse_db_id_dict.get(key)
        #     if reverse_key is not None:
        #         hri_data["hri_id"] = reverse_key
        #         new_data["hri_data"][reverse_key] = hri_data
        #     else:
        #         raise Exception(f"HRIs not found for {key}")

        return new_data


# pprint.pprint(HighRiskItem().get_hri_final_data())
# pprint.pprint(HighRiskItem().support())
# pprint.pprint(HighRiskItem().cloudwatch())
# pprint.pprint(HighRiskItem().s3())
# pprint.pprint(HighRiskItem().iam())
# pprint.pprint(HighRiskItem().config())
# pprint.pprint(HighRiskItem().cloudtrail())
# pprint.pprint(HighRiskItem().ec2())
# pprint.pprint(HighRiskItem().elasticache())
# pprint.pprint(HighRiskItem().dynamodb())
# pprint.pprint(HighRiskItem().rds())
# pprint.pprint(HighRiskItem().elbv2())
# pprint.pprint(HighRiskItem().aws_lambda())
hri = HighRiskItem()

ec2 = boto3.client("ec2", **hri.AWS_CREDS)
response = ec2.describe_regions()
region_names = [region['RegionName'] for region in response['Regions']]
region = region_names


# threads = []
# for region in regions:
#     thread = threading.Thread(target=self.s3, args=(region,))
#     thread.start()
#     threads.append(thread)

# num_threads = threading.active_count()
# print(f" {num_threads} threads running.")
# # Wait for all threads to finish before continuing
# for thread in threads:
#     thread.join()

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(hri.get_hri_final_data, region)
               for region in region_names]
    concurrent.futures.wait(futures)

    # hri.get_hri_final_data(region)

# pprint.pprint(HighRiskItem().get_hri_format())
# pprint.pprint(HighRiskItem().get_hri_final_data())