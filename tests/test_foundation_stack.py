from aws_cdk.assertions import Match


def test_budget_alerts_at_40_80_110_without_credits(template):
    t = template("FraudFoundation")
    t.resource_count_is("AWS::Budgets::Budget", 1)
    t.has_resource_properties(
        "AWS::Budgets::Budget",
        {
            "Budget": Match.object_like(
                {
                    "BudgetType": "COST",
                    "TimeUnit": "ANNUALLY",
                    "BudgetLimit": {"Amount": 134, "Unit": "USD"},
                    "CostTypes": Match.object_like({"IncludeCredit": False}),
                }
            ),
        },
    )
    budget = next(iter(t.find_resources("AWS::Budgets::Budget").values()))
    notes = budget["Properties"]["NotificationsWithSubscribers"]
    assert sorted(n["Notification"]["Threshold"] for n in notes) == [40, 80, 110]
    assert all(n["Subscribers"][0]["Address"] == "sahilcsgo69@gmail.com" for n in notes)


def test_buckets_are_private_encrypted_and_destroyable(template):
    t = template("FraudFoundation")
    t.resource_count_is("AWS::S3::Bucket", 3)
    for bucket in t.find_resources("AWS::S3::Bucket").values():
        props = bucket["Properties"]
        assert props["PublicAccessBlockConfiguration"]["BlockPublicAcls"] is True
        assert "BucketEncryption" in props
        assert bucket["DeletionPolicy"] == "Delete"


def test_athena_results_expire(template):
    template("FraudFoundation").has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketName": Match.string_like_regexp("athena-results"),
            "LifecycleConfiguration": {"Rules": [Match.object_like({"ExpirationInDays": 7})]},
        },
    )


def test_glue_database(template):
    template("FraudFoundation").has_resource_properties(
        "AWS::Glue::Database",
        {"DatabaseInput": Match.object_like({"Name": "fraud_analyst_lake"})},
    )
