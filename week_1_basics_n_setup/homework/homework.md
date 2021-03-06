## Week 1 Homework

In this homework we'll prepare the environment 
and practice with terraform and SQL

## Question 1. Google Cloud SDK

Install Google Cloud SDK. What's the version you have? 

To get the version, run `gcloud --version`



Google Cloud SDK 369.0.0



## Google Cloud account 

Create an account in Google Cloud and create a project.


## Question 2. Terraform 

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

* `terraform init`
* `terraform plan`
* `terraform apply` 

Apply the plan and copy the output to the form

var.project
  Your GCP Project ID

  Enter a value: innate-temple-338718


Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
  + resource "google_bigquery_dataset" "dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "trips_data_all"
      + delete_contents_on_destroy = false
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "europe-west6"
      + project                    = "innate-temple-338718"
      + self_link                  = (known after apply)

      + access {
          + domain         = (known after apply)
          + group_by_email = (known after apply)
          + role           = (known after apply)
          + special_group  = (known after apply)
          + user_by_email  = (known after apply)

          + view {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + table_id   = (known after apply)
            }
        }
    }

  # google_storage_bucket.data-lake-bucket will be created
  + resource "google_storage_bucket" "data-lake-bucket" {
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "EUROPE-WEST6"
      + name                        = "dtc_data_lake_innate-temple-338718"
      + project                     = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }

          + condition {
              + age                   = 30
              + matches_storage_class = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
google_storage_bucket.data-lake-bucket: Creation complete after 1s [id=dtc_data_lake_innate-temple-338718]
google_bigquery_dataset.dataset: Creation complete after 3s [id=projects/innate-temple-338718/datasets/trips_data_all]


## Prepare Postgres 

Run Postgres and load data as shown in the videos

We'll use the yellow taxi trips from January 2021:

```bash
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
```

Download this data and put it to Postgres

## Question 3. Count records 

How many taxi trips were there on January 15?

SELECT
	COUNT(1)
FROM yellow_taxi_data
WHERE DATE(tpep_pickup_datetime) = '2021-01-15'

53024

## Question 4. Average

Find the largest tip for each day. 
On which day it was the largest tip in January?

(note: it's not a typo, it's "tip", not "trip")

SELECT
	date_trip
FROM
(SELECT
	MAX(tip_amount) AS tip,
	DATE(tpep_pickup_datetime) AS date_trip
FROM yellow_taxi_data
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-01-31'
GROUP BY DATE(tpep_pickup_datetime)) as query_in
WHERE tip = (SELECT MAX(tip) FROM (SELECT
	MAX(tip_amount) AS tip,
	DATE(tpep_pickup_datetime) AS date_trip
FROM yellow_taxi_data
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-01-31'
GROUP BY DATE(tpep_pickup_datetime)) AS query_in2)

2021-01-20

OR   

SELECT
	MAX(tip_amount) AS tip,
	DATE(tpep_pickup_datetime) AS date_trip
FROM public.yellow_taxi_data
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-01-31'
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY tip desc
LIMIT(1)

tip: 1140.44 | day: 2021-01-20

## Question 5. Most popular destination

What was the most popular destination for passengers picked up 
in central park on January 14?

Enter the district name (not id)

SELECT
	COUNT(1) AS naumbers_trips,
	zdo."Zone"
FROM
	yellow_taxi_data t
JOIN zones zpu
ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo
ON t."DOLocationID" = zdo."LocationID"
WHERE DATE(tpep_pickup_datetime) = '2021-01-14' AND zpu."Zone" = 'Central Park'
GROUP BY zdo."Zone"
ORDER BY naumbers_trips DESC
LIMIT(1)

trips: 97  |  Zone: Upper East Side South


## Question 6. 

What's the pickup-dropoff pair with the largest 
average price for a ride (calculated based on `total_amount`)?


SELECT
	zpu."Zone",
	zdo."Zone",
	AVG(total_amount) 
FROM
	yellow_taxi_data t
JOIN zones zpu
ON t."PULocationID" = zpu."LocationID"
JOIN zones zdo
ON t."DOLocationID" = zdo."LocationID"
GROUP BY zpu."Zone", zdo."Zone"
ORDER BY AVG(total_amount) DESC
LIMIT(1)

pair zones: Alphabet City / Unknown  |  average price: 2292.4

