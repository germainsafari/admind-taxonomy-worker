My question
i found out that i have 2 subscription. one is free another one is one i bought myself. I don't know which one this app project is using so that i can stop the other one before it renews. can you help me here?

additionally, I was able to fetch some projects from scoro using cli and that python cloud run job however, this process is triggered manually and i want all of this to be automatic, from fetching projects from scoro and add them to bigquery, to syncing data from gemini enterprise to bigquery, then transfering data to firestore. I added a trigger but it looks like it was not running at all. please help me with this request by setting up an automated workflow for this. There is already a trigger on that worker but is is not working since the time i created it above as you showed me earlier.

When i click on execute the job, it only sync one project
DEFAULT 2026-06-09T07:43:24.343995Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_1439", "source_document_count": 2}
  {
    "textPayload": "INFO __main__ {\"status\": \"ok\", \"job\": \"wiki-generate\", \"project_id\": \"scoro_1439\", \"source_document_count\": 2}",
    "insertId": "6a27c41c00053fbb22c8b08b",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "job_name": "admind-taxonomy-worker",
        "project_id": "admind-data-organisation",
        "location": "europe-west1"
      }
    },
    "timestamp": "2026-06-09T07:43:24.343995Z",
    "labels": {
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-b6n8h",
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/task_index": "0",
      "instanceId": "002f8ffd473d76a10aeffba2ad915a4134bd348c17b2293a2591c38d2f568140cb08ac31ddb789b33a358514969a661b894890493f1ef172f01c819ec719a9cc86ba67de5a1e0dc02db4ce159a3846750308eaa2d9f655b06232f98f"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T07:43:24.348436391Z"
  }
INFO 2026-06-09T07:43:26.950441Z Container called exit(0).
  {
    "textPayload": "Container called exit(0).",
    "insertId": "6a27c41e000e8174a16d7d59",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1",
        "project_id": "admind-data-organisation"
      }
    },
    "timestamp": "2026-06-09T07:43:26.950441Z",
    "severity": "INFO",
    "labels": {
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-b6n8h",
      "instanceId": "002f8ffd473d76a10aeffba2ad915a4134bd348c17b2293a2591c38d2f568140cb08ac31ddb789b33a358514969a661b894890493f1ef172f01c819ec719a9cc86ba67de5a1e0dc02db4ce159a3846750308eaa2d9f655b06232f98f",
      "container_name": "admind-taxonomy-worker-1",
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/task_index": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fvarlog%2Fsystem",
    "receiveTimestamp": "2026-06-09T07:43:26.956010338Z"
  }

however, running via gcloud CLI runs a lot of jobs but not all of them or it mostly likely did not find related data and i don't know why. also even the existing wiki are not deep, the lack a lot of info and context, it looks. can you verify the code and see if there's something i can do to improve this? also it fetched few projects from scoro. why

gcloud run jobs update admind-taxonomy-worker --region europe-west1 \
  --update-env-vars JOB_TYPE=full-sync
gcloud run jobs execute admind-taxonomy-worker --region europe-west1 --wait
Updating Cloud Run job [admind-taxonomy-worker] in project [admind-data-organisation] region [europe-west1]
Updating job...                                                                                                                                                                                                                          
Done.                                                                                                                                                                                                                                    
Job [admind-taxonomy-worker] has successfully been updated.

To execute this job, use:
gcloud run jobs execute admind-taxonomy-worker
Creating execution...                                                                                                                                                                                                                    
  Provisioning resources... Provisioned imported containers....done                                                                                                                                                                      
  Starting execution... Started deployed execution in 15.95s....done                                                                                                                                                                     
  Running execution... 1 / 1 complete...done                                                                                                                                                                                             
Done.                                                                                                                                                                                                                                    
Execution [admind-taxonomy-worker-7w7n7] has successfully completed.

View details about this execution by running:
gcloud run jobs executions describe admind-taxonomy-worker-7w7n7

Or visit https://console.cloud.google.com/run/jobs/executions/details/europe-west1/admind-taxonomy-worker-7w7n7?project=493121771508
germain_safari@cloudshell:~ (admind-data-organisation)$ 


what i got
DEFAULT 2026-06-09T08:48:26.479544Z INFO __main__ full-sync: starting step scoro-sync
  {
    "textPayload": "INFO __main__ full-sync: starting step scoro-sync",
    "insertId": "6a27d35a00075138b58270b3",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1"
      }
    },
    "timestamp": "2026-06-09T08:48:26.479544Z",
    "labels": {
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "run.googleapis.com/task_index": "0",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:48:26.485199470Z"
  }
DEFAULT 2026-06-09T08:48:26.479562Z INFO __main__ Scoro POST https://admindagency.scoro.com/api/v2/projects/list
  {
    "textPayload": "INFO __main__ Scoro POST https://admindagency.scoro.com/api/v2/projects/list",
    "insertId": "6a27d35a0007514a8a0ebabb",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "location": "europe-west1",
        "job_name": "admind-taxonomy-worker"
      }
    },
    "timestamp": "2026-06-09T08:48:26.479562Z",
    "labels": {
      "run.googleapis.com/task_index": "0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:48:26.485199470Z"
  }
DEFAULT 2026-06-09T08:48:31.912380Z INFO __main__ {"status": "ok", "job": "scoro-sync", "projects_upserted": 26}
  {
    "textPayload": "INFO __main__ {\"status\": \"ok\", \"job\": \"scoro-sync\", \"projects_upserted\": 26}",
    "insertId": "6a27d35f000debfcf5594e60",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "location": "europe-west1",
        "job_name": "admind-taxonomy-worker",
        "project_id": "admind-data-organisation"
      }
    },
    "timestamp": "2026-06-09T08:48:31.912380Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/task_index": "0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:48:32.145751606Z"
  }
DEFAULT 2026-06-09T08:48:32.275452Z INFO __main__ full-sync: starting step document-discovery
  {
    "textPayload": "INFO __main__ full-sync: starting step document-discovery",
    "insertId": "6a27d360000433fcfce68085",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "location": "europe-west1",
        "job_name": "admind-taxonomy-worker",
        "project_id": "admind-data-organisation"
      }
    },
    "timestamp": "2026-06-09T08:48:32.275452Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/task_index": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:48:32.480841309Z"
  }
DEFAULT 2026-06-09T08:48:44.173714Z INFO __main__ document-discovery: IRB 5500 packaging_Chelsea Hsu_Chankluan R → 10 docs
  {
    "textPayload": "INFO __main__ document-discovery: IRB 5500 packaging_Chelsea Hsu_Chankluan R → 10 docs",
    "insertId": "6a27d36c0002a692969dcdfb",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "location": "europe-west1",
        "job_name": "admind-taxonomy-worker"
      }
    },
    "timestamp": "2026-06-09T08:48:44.173714Z",
    "labels": {
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/task_index": "0",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:48:44.466090923Z"
  }
DEFAULT 2026-06-09T08:48:55.560350Z INFO __main__ document-discovery: ABB_CF_Workday Banners for HRXP_Adela Talbot_Grzelak K → 10 docs
  {
    "textPayload": "INFO __main__ document-discovery: ABB_CF_Workday Banners for HRXP_Adela Talbot_Grzelak K → 10 docs",
    "insertId": "6a27d37700088cde7dc7826d",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "location": "europe-west1",
        "job_name": "admind-taxonomy-worker"
      }
    },
    "timestamp": "2026-06-09T08:48:55.560350Z",
    "labels": {
      "run.googleapis.com/task_index": "0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:48:55.787409921Z"
  }
DEFAULT 2026-06-09T08:49:06.353573Z INFO __main__ document-discovery: Alvernia Planet_brand strategy, visual identity_Krzysztof Krukowski_Sadecka D → 10 docs
  {
    "textPayload": "INFO __main__ document-discovery: Alvernia Planet_brand strategy, visual identity_Krzysztof Krukowski_Sadecka D → 10 docs",
    "insertId": "6a27d382000565255215fe08",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1"
      }
    },
    "timestamp": "2026-06-09T08:49:06.353573Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/task_index": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:49:06.357191293Z"
  }
DEFAULT 2026-06-09T08:49:18.410977Z INFO __main__ document-discovery: Datwyler_brand activation_David Friedman_Czerw D → 10 docs
  {
    "textPayload": "INFO __main__ document-discovery: Datwyler_brand activation_David Friedman_Czerw D → 10 docs",
    "insertId": "6a27d38e000645610c3b4581",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1",
        "project_id": "admind-data-organisation"
      }
    },
    "timestamp": "2026-06-09T08:49:18.410977Z",
    "labels": {
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "run.googleapis.com/task_index": "0",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:49:18.414644439Z"
  }
DEFAULT 2026-06-09T08:49:30.342851Z INFO __main__ document-discovery: Admind CT_Agents development_Florek → 10 docs
  {
    "textPayload": "INFO __main__ document-discovery: Admind CT_Agents development_Florek → 10 docs",
    "insertId": "6a27d39a00053b4377956fcb",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1"
      }
    },
    "timestamp": "2026-06-09T08:49:30.342851Z",
    "labels": {
      "run.googleapis.com/task_index": "0",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:49:30.401618586Z"
  }
DEFAULT 2026-06-09T08:49:40.011783Z INFO __main__ document-discovery: 313_PPT_Crealizer status_Elena Canas Gonzalez_Zmuda E → 10 docs
  {
    "textPayload": "INFO __main__ document-discovery: 313_PPT_Crealizer status_Elena Canas Gonzalez_Zmuda E → 10 docs",
    "insertId": "6a27d3a400002e07d0e44d7d",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1",
        "project_id": "admind-data-organisation"
      }
    },
    "timestamp": "2026-06-09T08:49:40.011783Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/task_index": "0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:49:40.062140795Z"
  }
DEFAULT 2026-06-09T08:49:51.013651Z INFO __main__ document-discovery: MV Titanium marketing collateral_Stevan Thorne_Kawecka D → 10 docs
DEFAULT 2026-06-09T08:50:02.792468Z INFO __main__ document-discovery: Sabic_Branding_Linsday Clarksmead_Witek B → 10 docs
  {
    "textPayload": "INFO __main__ document-discovery: Sabic_Branding_Linsday Clarksmead_Witek B → 10 docs",
    "insertId": "6a27d3ba000c17948a6f8662",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1"
      }
    },
    "timestamp": "2026-06-09T08:50:02.792468Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/task_index": "0",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:50:03.008472094Z"
  }
DEFAULT 2026-06-09T08:50:14.347977Z INFO __main__ document-discovery: Teatr Stary Website_Anna Słowiakowska_Flisikowska A → 10 docs
DEFAULT 2026-06-09T08:50:23.684739Z INFO __main__ document-discovery: B Corp Parapetówka 11.03.2026 → 10 docs
  {
    "textPayload": "INFO __main__ document-discovery: B Corp Parapetówka 11.03.2026 → 10 docs",
    "insertId": "6a27d3cf000a72c3ae051cc9",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1",
        "project_id": "admind-data-organisation"
      }
    },
    "timestamp": "2026-06-09T08:50:23.684739Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/task_index": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:50:23.985171697Z"
  }
DEFAULT 2026-06-09T08:50:33.702540Z INFO __main__ document-discovery: 317_PPT_Overview MnPC_Josef Koell_Zmuda E → 10 docs
DEFAULT 2026-06-09T08:50:45.277623Z INFO __main__ document-discovery: Tesar_Offer preparation_Michalska A → 10 docs
DEFAULT 2026-06-09T08:50:55.314622Z INFO __main__ document-discovery: Kreatywnie Dla Zdrowia_strategia komunikacji, branding kampanii_Michał Jędrzejek_Sadecka D → 10 docs
DEFAULT 2026-06-09T08:51:07.288225Z INFO __main__ document-discovery: Internal Zapier automations_Makowska Aleksandra_Krawczyk R → 10 docs
DEFAULT 2026-06-09T08:51:17.811938Z INFO __main__ document-discovery: ABB Robotics Channel Partners guidelines_Laura Gamboa_Ostrowski J → 10 docs
DEFAULT 2026-06-09T08:51:26.779485Z INFO __main__ document-discovery: Tauron Arena Pitch concept → 10 docs
DEFAULT 2026-06-09T08:51:38.847907Z INFO __main__ document-discovery: Facility Branding Senatobia_Dawid Kalinowski_Constantina Rizou → 10 docs
  {
    "textPayload": "INFO __main__ document-discovery: Facility Branding Senatobia_Dawid Kalinowski_Constantina Rizou → 10 docs",
    "insertId": "6a27d41a000cf0238e46847d",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "location": "europe-west1",
        "job_name": "admind-taxonomy-worker"
      }
    },
    "timestamp": "2026-06-09T08:51:38.847907Z",
    "labels": {
      "run.googleapis.com/task_index": "0",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:51:38.896693774Z"
  }
DEFAULT 2026-06-09T08:51:44.994729Z INFO __main__ document-discovery: BREEAM AEM LP_Beatriz Martinez De Olcoz_Szymczak V → 0 docs
  {
    "textPayload": "INFO __main__ document-discovery: BREEAM AEM LP_Beatriz Martinez De Olcoz_Szymczak V → 0 docs",
    "insertId": "6a27d420000f2da900880699",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1",
        "project_id": "admind-data-organisation"
      }
    },
    "timestamp": "2026-06-09T08:51:44.994729Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/task_index": "0",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:51:45.223132127Z"
  }
DEFAULT 2026-06-09T08:51:55.150272Z INFO __main__ document-discovery: ABB_AOT Niklas Campe_Zyla A → 10 docs
DEFAULT 2026-06-09T08:52:04.858655Z INFO __main__ document-discovery: Admind_BrandOS_Bartek Witek_Witek B → 10 docs
  {
    "textPayload": "INFO __main__ document-discovery: Admind_BrandOS_Bartek Witek_Witek B → 10 docs",
    "insertId": "6a27d434000d1a1fdb441c04",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1",
        "project_id": "admind-data-organisation"
      }
    },
    "timestamp": "2026-06-09T08:52:04.858655Z",
    "labels": {
      "run.googleapis.com/task_index": "0",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:52:04.872879911Z"
  }
DEFAULT 2026-06-09T08:52:04.858666Z INFO __main__ {"status": "ok", "job": "document-discovery", "projects_searched": 20, "documents_discovered": 190}
DEFAULT 2026-06-09T08:52:04.923751Z INFO __main__ full-sync: starting step taxonomy-sync
  {
    "textPayload": "INFO __main__ full-sync: starting step taxonomy-sync",
    "insertId": "6a27d434000e1867c48a81ff",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1",
        "project_id": "admind-data-organisation"
      }
    },
    "timestamp": "2026-06-09T08:52:04.923751Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/task_index": "0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:52:05.206030889Z"
  }
DEFAULT 2026-06-09T08:54:05.735092Z INFO __main__ {"status": "ok", "job": "taxonomy-sync", "run_id": "a2fc51b7-5526-451d-9209-d93f1933756a", "projects_processed": 20, "documents_processed": 10, "mappings_created": 1}
DEFAULT 2026-06-09T08:54:05.825693Z INFO __main__ full-sync: starting step wiki-generate
  {
    "textPayload": "INFO __main__ full-sync: starting step wiki-generate",
    "insertId": "6a27d4ad000c995d5e1f703b",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1"
      }
    },
    "timestamp": "2026-06-09T08:54:05.825693Z",
    "labels": {
      "run.googleapis.com/task_index": "0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:54:06.071990744Z"
  }
DEFAULT 2026-06-09T08:54:11.464790Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_2302", "source_document_count": 0}
  {
    "textPayload": "INFO __main__ {\"status\": \"ok\", \"job\": \"wiki-generate\", \"project_id\": \"scoro_2302\", \"source_document_count\": 0}",
    "insertId": "6a27d4b300071796332f3697",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "location": "europe-west1",
        "job_name": "admind-taxonomy-worker"
      }
    },
    "timestamp": "2026-06-09T08:54:11.464790Z",
    "labels": {
      "run.googleapis.com/task_index": "0",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:54:11.740563236Z"
  }
DEFAULT 2026-06-09T08:54:14.851562Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_2292", "source_document_count": 0}
DEFAULT 2026-06-09T08:54:18.395551Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_1499", "source_document_count": 0}
  {
    "textPayload": "INFO __main__ {\"status\": \"ok\", \"job\": \"wiki-generate\", \"project_id\": \"scoro_1499\", \"source_document_count\": 0}",
    "insertId": "6a27d4ba0006091ff619ea53",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "location": "europe-west1",
        "job_name": "admind-taxonomy-worker"
      }
    },
    "timestamp": "2026-06-09T08:54:18.395551Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/task_index": "0",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:54:18.726260750Z"
  }
DEFAULT 2026-06-09T08:54:22.523139Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_1707", "source_document_count": 0}
DEFAULT 2026-06-09T08:54:27.328662Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_1929", "source_document_count": 1}
  {
    "textPayload": "INFO __main__ {\"status\": \"ok\", \"job\": \"wiki-generate\", \"project_id\": \"scoro_1929\", \"source_document_count\": 1}",
    "insertId": "6a27d4c3000503d64385f89a",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1"
      }
    },
    "timestamp": "2026-06-09T08:54:27.328662Z",
    "labels": {
      "run.googleapis.com/task_index": "0",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:54:27.385138423Z"
  }
DEFAULT 2026-06-09T08:54:31.247399Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_2304", "source_document_count": 0}
DEFAULT 2026-06-09T08:54:34.650956Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_357", "source_document_count": 0}
  {
    "textPayload": "INFO __main__ {\"status\": \"ok\", \"job\": \"wiki-generate\", \"project_id\": \"scoro_357\", \"source_document_count\": 0}",
    "insertId": "6a27d4ca0009eecc5abdcba3",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "location": "europe-west1",
        "job_name": "admind-taxonomy-worker"
      }
    },
    "timestamp": "2026-06-09T08:54:34.650956Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/task_index": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:54:34.711189533Z"
  }
DEFAULT 2026-06-09T08:54:38.334304Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_1989", "source_document_count": 0}
DEFAULT 2026-06-09T08:54:41.847588Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_2285", "source_document_count": 0}
  {
    "textPayload": "INFO __main__ {\"status\": \"ok\", \"job\": \"wiki-generate\", \"project_id\": \"scoro_2285\", \"source_document_count\": 0}",
    "insertId": "6a27d4d1000ceee4319a8081",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "location": "europe-west1",
        "job_name": "admind-taxonomy-worker",
        "project_id": "admind-data-organisation"
      }
    },
    "timestamp": "2026-06-09T08:54:41.847588Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/task_index": "0",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:54:42.037333295Z"
  }
DEFAULT 2026-06-09T08:54:55.864893Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_1439", "source_document_count": 2}
DEFAULT 2026-06-09T08:54:58.484820Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_2319", "source_document_count": 0}
DEFAULT 2026-06-09T08:55:02.087383Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_1709", "source_document_count": 0}
  {
    "textPayload": "INFO __main__ {\"status\": \"ok\", \"job\": \"wiki-generate\", \"project_id\": \"scoro_1709\", \"source_document_count\": 0}",
    "insertId": "6a27d4e60001555754216d56",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "project_id": "admind-data-organisation",
        "location": "europe-west1",
        "job_name": "admind-taxonomy-worker"
      }
    },
    "timestamp": "2026-06-09T08:55:02.087383Z",
    "labels": {
      "run.googleapis.com/task_attempt": "0",
      "run.googleapis.com/task_index": "0",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:55:02.199786947Z"
  }
DEFAULT 2026-06-09T08:55:05.549749Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_2018", "source_document_count": 0}
DEFAULT 2026-06-09T08:55:08.826714Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_1754", "source_document_count": 0}
DEFAULT 2026-06-09T08:55:14.515031Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_2215", "source_document_count": 1}
DEFAULT 2026-06-09T08:55:19.339858Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_1573", "source_document_count": 0}
  {
    "textPayload": "INFO __main__ {\"status\": \"ok\", \"job\": \"wiki-generate\", \"project_id\": \"scoro_1573\", \"source_document_count\": 0}",
    "insertId": "6a27d4f700052f92d1a53966",
    "resource": {
      "type": "cloud_run_job",
      "labels": {
        "job_name": "admind-taxonomy-worker",
        "location": "europe-west1",
        "project_id": "admind-data-organisation"
      }
    },
    "timestamp": "2026-06-09T08:55:19.339858Z",
    "labels": {
      "run.googleapis.com/task_index": "0",
      "run.googleapis.com/execution_name": "admind-taxonomy-worker-7w7n7",
      "instanceId": "002f8ffd4775bbf67cf794fde161628d4c2e61fea21ab48cf9abfc2876f3af770c82e10ff6beeb0d96362ebda9cc72c9ab2a1aaa7b6b27ade58d1cd993f38b0732fb8f1a8ad4e2f2e4709b5774acca6edde1c1e8b7bb01a49fb1f3d0",
      "run.googleapis.com/task_attempt": "0"
    },
    "logName": "projects/admind-data-organisation/logs/run.googleapis.com%2Fstderr",
    "receiveTimestamp": "2026-06-09T08:55:19.511657669Z"
  }
DEFAULT 2026-06-09T08:55:22.535005Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_2275", "source_document_count": 0}
DEFAULT 2026-06-09T08:55:26.352697Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_2265", "source_document_count": 0}
DEFAULT 2026-06-09T08:55:29.731574Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_1999", "source_document_count": 0}
DEFAULT 2026-06-09T08:55:33.765912Z INFO __main__ {"status": "ok", "job": "wiki-generate", "project_id": "scoro_2136", "source_document_count": 0}
DEFAULT 2026-06-09T08:55:33.811327Z INFO __main__ {"status": "ok", "job": "full-sync"}
INFO 2026-06-09T08:55:36.407814Z Container called exit(0).


I also managed to get data from firestore to deep wiki web app. as you can see in second and third attached image. however, clicking on project, it shows no data. i will attach a screenshot from firestore and bigquery but you can also suggest queries to run so that i can check this but help me improve the prompt to gemini so that i can bring good data from gemini enterprise and also use scoro mcp data store. The wiki should contain files.


on image 4, explain those service account and what does it mean


i was tring to deploy this project on render or vercel as i thought i was the easies approach. i got as error like this

Build-time prerender failure: The homepage uses revalidate = 60 which causes Next.js to attempt a static render at build time — but BigQuery, Firestore, and Vertex AI all need Google credentials, which don't exist in the Vercel/Render build environment.
Missing runtime credentials: Even at runtime, neither platform has Google Application Default Credentials configured.

cursor suggested to me that i should add 
Option A — Reuse an existing one (best if you already have Cloud Run)
Open: Service Accounts
Look for a service account already used by your backend/Cloud Run worker
Names often look like: ...@admind-data-organisation.iam.gserviceaccount.com
Click that service account

does this solve the problem? is it also good for us to deploy this project here or we can use GCP? i was thinking we don't need to pay that much on hosting since vercel is free and it will only fetch data from firestore that already existed. how much can i estimate for google cloud run if i deploy there compared to these free instances

Read carefully what i did above and guide me properly with this implementation.


Called tool
Thought for 1m 52s

You have actually made very good progress. The system is running end-to-end now, but there are three separate issues mixed together:

1. Licensing: which Gemini Enterprise subscription is actually being used?
2. Automation: why the trigger/scheduler is not running correctly.
3. Quality: why the wiki pages are shallow or have zero source docs.

I will go through them in that order.

1. Gemini Enterprise subscription: which one is being used?

From your screenshot, you have two Gemini Enterprise Plus subscriptions:

Subscription 1:
  Total licenses: 1
  Assigned: 0
  Renewal: Renews on Jun 28, 2026

Subscription 2:
  free_trial_gemini
  Total licenses: 50
  Assigned: 2
  Renewal: Expires on Jun 26, 2026

The important part is Assigned.

Your users table shows two assigned users:

germain.safari@admindagency.com
project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com

Both show:

Gemini Enterprise Plus (Expires on: Jun 26, 2026)

So the app is currently using the free_trial_gemini subscription, not the paid one. The paid subscription has 0 assigned licenses, so it is not actively assigned to your user or the service account.

My recommendation:

Keep free_trial_gemini while testing.
Cancel or disable renewal for the paid subscription if you do not want it to renew.
Only keep the paid one if Google requires it after the trial expires.

Do not remove the free-trial assignments yet because Discovery Engine workspace search is currently working because both your user and the service account are licensed.

2. What the service accounts in image 4 mean

You have three service accounts:

ais-gemini-key-011b1...
493121771508-compute@developer.gserviceaccount.com
project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com

Use them like this:

project-intelligence-worker@...
  This is your main production identity.
  Use this for Cloud Run Job, Cloud Scheduler, BigQuery, Firestore, Vertex AI,
  Discovery Engine, Scoro secret access, and domain-wide delegation.

493121771508-compute@developer.gserviceaccount.com
  This is the default Compute Engine service account.
  Avoid using it for this project. It is generic and harder to audit.

ais-gemini-key-...
  This looks like an automatically created service account for Gemini API / AI Studio key setup.
  Do not use it for the pipeline unless you intentionally designed around it.

So for your setup, the correct service account is:

project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com

Google Cloud Run Jobs support assigning a service identity to the job, which is exactly why this account exists. It lets the job call BigQuery, Firestore, Vertex AI, Discovery Engine, and Secret Manager without using your personal account.

3. Why clicking “Execute” only generated one project

This log:

wiki-generate project_id=scoro_1439 source_document_count=2

means that at that moment your job was probably configured as:

JOB_TYPE=wiki-generate-one
PROJECT_ID_TO_GENERATE=scoro_1439

or a previous job config still had a single-project setting.

When you later ran:

gcloud run jobs update admind-taxonomy-worker --region europe-west1 \
  --update-env-vars JOB_TYPE=full-sync

then executed it, the job correctly ran the full pipeline:

scoro-sync
document-discovery
taxonomy-sync
wiki-generate

Your full-sync log confirms this:

scoro-sync → 26 projects
document-discovery → 20 projects searched, 190 documents discovered
taxonomy-sync → 20 projects processed, 10 documents processed, 1 mapping created
wiki-generate → many projects
full-sync → success

So Cloud Run itself is working.

The real problem is this line:

taxonomy-sync: documents_processed = 10, mappings_created = 1

That explains why most wiki pages are empty.

4. Main code problem: taxonomy is only classifying 10 documents

Your worker discovered 190 documents, but then taxonomy classified only 10 documents:

document-discovery: documents_discovered = 190
taxonomy-sync: documents_processed = 10

In the code, get_candidate_documents() does this:

SELECT *
FROM documents
LIMIT @limit

and taxonomy_sync() uses:

documents = get_candidate_documents(limit=int(os.getenv("DOCUMENT_LIMIT", "200")))

So if DOCUMENT_LIMIT=10, taxonomy only sees 10 global documents. Worse, they are not necessarily the documents found for the specific project. It classifies every project against the same first 10 rows from documents.

That is why you get:

20 projects searched
190 docs discovered
only 1 document mapped
many wiki pages with 0 source docs

The architecture needs one missing table or column:

document_discovery_candidates

or add this to documents:

candidate_project_id

The best approach is to create a separate table.

5. Add a project_document_candidates table

Run this in BigQuery:

CREATE TABLE IF NOT EXISTS `admind-data-organisation.admind_data_organisation.project_document_candidates` (
  project_id STRING,
  document_id STRING,
  discovery_query STRING,
  rank INT64,
  title STRING,
  url STRING,
  text_preview STRING,
  discovered_at TIMESTAMP,
  run_id STRING
);

This table means:

Discovery Engine returned this document as a candidate for this project.

Then your pipeline becomes:

document-discovery
  writes documents
  writes project_document_candidates

taxonomy-sync
  for each project:
    reads only candidates for that project
    classifies those candidates
    writes project_document_map

This will dramatically improve mapping quality.

6. Change taxonomy so it does not classify global random documents

Replace get_candidate_documents() with a project-scoped version:

def get_candidate_documents_for_project(project_id: str, limit: int = 50):
    sql = f"""
    SELECT d.*
    FROM `{PROJECT_ID}.{DATASET}.project_document_candidates` c
    JOIN `{PROJECT_ID}.{DATASET}.documents` d
      ON c.document_id = d.document_id
    WHERE c.project_id = @project_id
    ORDER BY c.rank ASC
    LIMIT @limit
    """
    return run_query(
        sql,
        [
            bigquery.ScalarQueryParameter("project_id", "STRING", project_id),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ],
    )

Then in taxonomy_sync():

for project in projects:
    project_id = _row(project, "project_id")
    documents = get_candidate_documents_for_project(
        project_id,
        limit=int(os.getenv("DOCUMENT_LIMIT", "50")),
    )

    if not documents:
        logger.info("taxonomy-sync: %s has no candidate documents", project_id)
        continue

    matches = classify_documents_for_project(project, documents)

This is the single most important code improvement.

7. Change document discovery to write candidates

Inside document_discovery(), after this:

docs = discover_documents_for_project(project)
upsert_documents(docs)

also write candidates:

def insert_document_candidates(project, docs, query, run_id):
    rows = []
    project_id = _row(project, "project_id")

    for idx, doc in enumerate(docs, start=1):
        rows.append({
            "project_id": project_id,
            "document_id": doc["document_id"],
            "discovery_query": query,
            "rank": idx,
            "title": doc.get("title", ""),
            "url": doc.get("url", ""),
            "text_preview": doc.get("text_preview", ""),
            "discovered_at": now_iso(),
            "run_id": run_id,
        })

    insert_rows("project_document_candidates", rows)

Then update discover_documents_for_project() to return both query and docs:

def discover_documents_for_project(project) -> tuple[str, list[dict]]:
    query = _build_project_query(project)
    if not query.strip():
        return query, []

    results = search_discovery_engine(query, page_size=int(os.getenv("DOCUMENT_LIMIT", "20")))
    ...
    return query, docs

Then in document_discovery():

query, docs = discover_documents_for_project(project)
upsert_documents(docs)
insert_document_candidates(project, docs, query, run_id)

Without this project-candidate table, your taxonomy will always be weak.

8. Why your wiki pages are shallow

There are three reasons.

First, most generated wiki pages have:

source_document_count = 0

The wiki writer cannot write a meaningful page without source documents. Your UI correctly shows “Not found in available sources.”

Second, your current documents.full_text contains only a snippet from Discovery Engine:

"full_text": snippet

So even when a document is mapped, the wiki receives maybe 1 short snippet, not the full PDF / DOCX / PPTX content.

Third, your Discovery Engine search request only asks for snippets:

"contentSearchSpec": {"snippetSpec": {"returnSnippet": True}}

The Search API supports content search options such as snippets and answer/search endpoints, but snippets are not enough for deep wiki generation.

9. Improve Discovery Engine retrieval

Update your search_discovery_engine() payload to request extractive content, not only snippets.

Try this payload:

payload = {
    "query": query,
    "pageSize": page_size,
    "spellCorrectionSpec": {"mode": "AUTO"},
    "languageCode": "en-US",
    "contentSearchSpec": {
        "snippetSpec": {
            "returnSnippet": True
        },
        "extractiveContentSpec": {
            "maxExtractiveAnswerCount": 3,
            "maxExtractiveSegmentCount": 3,
            "returnExtractiveSegmentScore": True
        }
    }
}

Then parse:

extractive_answers = derived.get("extractive_answers", []) or derived.get("extractiveAnswers", [])
extractive_segments = derived.get("extractive_segments", []) or derived.get("extractiveSegments", [])

And build full_text from all snippets/segments:

parts = []

if snippet:
    parts.append(snippet)

for ans in extractive_answers:
    content = ans.get("content") or ans.get("pageContent")
    if content:
        parts.append(content)

for seg in extractive_segments:
    content = seg.get("content") or seg.get("pageContent")
    if content:
        parts.append(content)

full_text = "\n\n".join(parts)

This will not equal full document extraction, but it should be much richer than one snippet.

10. Improve the discovery query

Your current query is:

project_name + project_no + client_company + google_drive_link

That is a good start, but many Scoro project names include people names and internal shorthand. For better retrieval, use multiple queries per project:

Query 1: project_name + project_no
Query 2: client_company + project_type + business_area
Query 3: important words from project_name without person names
Query 4: google_drive_link if available

For example:

def build_project_queries(project) -> list[str]:
    project_name = _row(project, "project_name")
    project_no = _row(project, "project_no")
    client = _row(project, "client_company")
    project_type = _row(project, "project_type")
    business_area = _row(project, "business_area")
    description = _row(project, "description")

    queries = []

    if project_name and project_no:
        queries.append(f'"{project_name}" "{project_no}"')

    if client and project_type:
        queries.append(f'"{client}" "{project_type}" "{business_area}"')

    if description:
        queries.append(f'"{client}" {description[:300]}')

    if project_name:
        cleaned = project_name.replace("_", " ")
        queries.append(cleaned)

    return [q for q in queries if q.strip()]

Then deduplicate documents by document_id.

This matters because Discovery Engine is search-based. Better query formulation means better candidate documents.

11. Improve the classifier prompt

Your current classifier is conservative:

Include only documents that clearly belong.
Do not guess.

That is safe, but it may reject useful candidates when snippets are short.

Use a two-level decision instead:

strong_match
possible_match
reject

Prompt:

You are a project-document taxonomy classifier for Admind Agency.

Your job is to decide whether each candidate document belongs to the project.

Project metadata:
- Project ID: {project_id}
- Project number: {project_no}
- Project name: {project_name}
- Client/company: {client_company}
- Project type: {project_type}
- Business area: {business_area}
- Manager/team: {project_manager}; {project_members}
- Dates: {start_date} to {due_date}
- Scoro description: {description}

Classification rules:
1. strong_match:
   Use when the document title/path/content clearly mentions the same project,
   project number, client, campaign, deliverable, or specific workstream.
2. possible_match:
   Use when the document appears related to the same client or workstream,
   but the evidence is incomplete.
3. reject:
   Use when the evidence is generic, unrelated, or only weakly matches.

Return ONLY valid JSON:
{
  "matches": [
    {
      "document_id": "...",
      "decision": "strong_match | possible_match | reject",
      "confidence_score": 0.0,
      "evidence": "quote or specific clue from title/path/content",
      "reason": "short explanation"
    }
  ]
}

Important:
- Do not classify generic brand guidelines as project-specific unless the project is about that brand guideline.
- Prefer project number, exact project name, client name + deliverable, and folder path evidence.
- If source evidence is weak, use possible_match, not strong_match.
- Include possible_match results with confidence >= 0.55.
- Include strong_match results with confidence >= 0.75.

Then save possible matches too, but mark them:

matching_method = gemini_possible_match
needs_human_review = true

You can add columns later:

ALTER TABLE `admind-data-organisation.admind_data_organisation.project_document_map`
ADD COLUMN IF NOT EXISTS decision STRING;

ALTER TABLE `admind-data-organisation.admind_data_organisation.project_document_map`
ADD COLUMN IF NOT EXISTS needs_human_review BOOL;
12. Improve wiki generation prompt

Your current wiki prompt is structurally fine, but it only gets snippets, so it writes shallow content. Once you pass richer source_docs, use this prompt:

You are an internal project intelligence documentarian for Admind Agency.

Create a DeepWiki-style internal project page.

Project:
- Project number: {project_no}
- Project name: {project_name}
- Client/company: {client_company}
- Project manager: {project_manager}
- Team: {project_members}
- Period: {start_date} to {due_date}
- Project type: {project_type}
- Business area: {business_area}
- Scoro description: {description}

Use ONLY the source documents below.
Do not invent facts.
If information is missing, write "Not found in available sources."

Write Markdown with these sections:

## Executive Summary
5 to 8 bullet points summarizing what this project is, why it exists, and current state.

## Project Scope
Describe deliverables, channels, brand/campaign scope, markets, and workstreams.

## Brief & Objectives
State business objectives and creative objectives separately.

## Timeline
List dates, milestones, approvals, meetings, and delivery moments.

## Team & Stakeholders
Separate Admind team, client stakeholders, and external partners if available.

## Key Decisions
List decisions with source citations.

## Design Assets & Deliverables
List files/assets found, with document titles and URLs where available.

## Meeting Intelligence
Summarize meeting notes, decisions, open questions, blockers, and next steps.

## Risks, Gaps & Unknowns
Flag missing documents, unclear ownership, low confidence, or incomplete source coverage.

## Source Map
Create a table:
| Source | Type | Why it matters | URL |

Citation rule:
For every factual claim, cite the document title in parentheses.
Example: "The project focuses on website refresh (KI Website Refresh Pitch 2025.pptx)."

Source documents:
{source_docs}

Also: if source_document_count = 0, do not call Gemini. Store a short placeholder wiki with status:

wiki_status = "no_sources"

This avoids generating lots of useless pages.

13. Fix wiki_generate_all() so it only generates useful pages

Currently it generates a wiki for every active project, even if a project has zero mapped documents. That is why the app shows many “Wiki available” pages with no real content.

Change:

def wiki_generate_all():
    projects = get_active_projects(limit=int(os.getenv("PROJECT_LIMIT", "20")))
    for project in projects:
        pid = _row(project, "project_id")
        if pid:
            generate_wiki_for_project(pid)

to:

def get_projects_with_mapped_documents(limit: int = 20):
    sql = f"""
    SELECT p.*, COUNT(m.document_id) AS mapped_document_count
    FROM `{PROJECT_ID}.{DATASET}.projects` p
    JOIN `{PROJECT_ID}.{DATASET}.project_document_map` m
      ON p.project_id = m.project_id
    WHERE LOWER(CAST(p.status AS STRING)) NOT IN ('done', 'completed', 'cancelled', 'closed')
    GROUP BY ALL
    ORDER BY mapped_document_count DESC
    LIMIT @limit
    """
    return run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])

If GROUP BY ALL gives trouble, explicitly group columns or first query project IDs.

Simpler:

def get_project_ids_with_mapped_documents(limit: int = 20):
    sql = f"""
    SELECT project_id, COUNT(*) AS mapped_document_count
    FROM `{PROJECT_ID}.{DATASET}.project_document_map`
    GROUP BY project_id
    HAVING mapped_document_count > 0
    ORDER BY mapped_document_count DESC
    LIMIT @limit
    """
    return run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])

Then:

def wiki_generate_all():
    rows = get_project_ids_with_mapped_documents(limit=int(os.getenv("PROJECT_LIMIT", "20")))
    for row in rows:
        generate_wiki_for_project(row.project_id)

This will make “Wiki available” mean there is actually source data.

14. Why Scoro fetched only 26 projects

Your Scoro code filters only:

"filter": {"status": "inprogress"}

So 26 means Scoro returned 26 projects with that status. That is consistent with your UI showing:

26 active projects

If you want more, change the filter. For example:

"filter": {
  "status": ["inprogress", "not_started", "pending"]
}

But this depends on Scoro’s exact accepted status values. The safer immediate test is to remove the filter temporarily and see how many Scoro returns:

data = _scoro_post("projects/list", {
    "page": page,
    "per_page": per_page
})

Then inspect statuses in BigQuery.

15. Automation: create a reliable Scheduler setup

Google Cloud Run Jobs are designed to be executed on a schedule through Cloud Scheduler.

First, set the job default to full-sync:

gcloud run jobs update admind-taxonomy-worker \
  --region europe-west1 \
  --update-env-vars JOB_TYPE=full-sync,PROJECT_LIMIT=20,DOCUMENT_LIMIT=50

Then grant the scheduler identity permission to run the job:

gcloud run jobs add-iam-policy-binding admind-taxonomy-worker \
  --region europe-west1 \
  --member="serviceAccount:project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com" \
  --role="roles/run.invoker"

Delete the old broken scheduler if needed:

gcloud scheduler jobs list --location europe-west1

If you see an old one, delete it:

gcloud scheduler jobs delete admind-taxonomy-worker-nightly \
  --location europe-west1

Create a clean scheduler:

gcloud scheduler jobs create http admind-taxonomy-worker-nightly \
  --location europe-west1 \
  --schedule "0 2 * * *" \
  --time-zone "Europe/Warsaw" \
  --uri "https://run.googleapis.com/v2/projects/admind-data-organisation/locations/europe-west1/jobs/admind-taxonomy-worker:run" \
  --http-method POST \
  --oauth-service-account-email project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com

Test it manually:

gcloud scheduler jobs run admind-taxonomy-worker-nightly \
  --location europe-west1

Then check:

gcloud run jobs executions list \
  --job admind-taxonomy-worker \
  --region europe-west1

And check BigQuery:

SELECT *
FROM `admind-data-organisation.admind_data_organisation.pipeline_runs`
ORDER BY started_at DESC
LIMIT 30;

If scheduler still does not run, check:

gcloud scheduler jobs describe admind-taxonomy-worker-nightly \
  --location europe-west1

Look for:

lastAttemptTime
state
status

Most scheduler failures are caused by wrong region, wrong URI, or missing roles/run.invoker.

16. Your BigQuery checks

Run these to understand the actual quality.

How many discovered docs?
SELECT
  source_system,
  COUNT(*) AS docs
FROM `admind-data-organisation.admind_data_organisation.documents`
GROUP BY source_system;
Which projects have mappings?
SELECT
  m.project_id,
  p.project_no,
  p.project_name,
  COUNT(*) AS mapped_docs
FROM `admind-data-organisation.admind_data_organisation.project_document_map` m
LEFT JOIN `admind-data-organisation.admind_data_organisation.projects` p
ON m.project_id = p.project_id
GROUP BY m.project_id, p.project_no, p.project_name
ORDER BY mapped_docs DESC;
Which wiki pages have zero sources?
SELECT
  job_name,
  status,
  records_read,
  records_written,
  error_message,
  started_at
FROM `admind-data-organisation.admind_data_organisation.pipeline_runs`
WHERE job_name = 'wiki-generate'
ORDER BY started_at DESC
LIMIT 50;
Check duplicates in mappings
SELECT
  project_id,
  document_id,
  COUNT(*) AS duplicate_count
FROM `admind-data-organisation.admind_data_organisation.project_document_map`
GROUP BY project_id, document_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
17. Deploying DeepWiki: Vercel/Render vs GCP

The Vercel/Render error makes sense.

Your app uses BigQuery, Firestore, and Vertex AI. During Next.js static prerendering, it tried to call Google APIs at build time, but the build environment had no Google credentials. Google’s Application Default Credentials are how Google client libraries find credentials in server environments; on Vercel/Render you must explicitly provide credentials or avoid calling Google APIs at build time.

Reusing the existing Cloud Run service account does not automatically solve the problem on Vercel. Service accounts work natively inside Google Cloud. Outside Google Cloud, you need one of these:

Option A:
  Export a service-account JSON key and store it as a Vercel secret.
  This works but is less secure because you now have a long-lived private key outside GCP.

Option B:
  Use Workload Identity Federation.
  More secure but more setup.

Option C:
  Deploy the Next.js app on Cloud Run.
  Best fit here because it can use the service account directly without JSON keys.

I recommend Cloud Run for this app, not Vercel, because:

Your data is in BigQuery + Firestore + Vertex AI.
Your auth model is already GCP IAM.
Your worker is already on Cloud Run.
You avoid exporting service account keys.

For costs: Cloud Run is pay-per-use and has a free tier. For services with request-based billing, Google lists 2 million requests/month free plus free CPU/RAM quotas; Cloud Run Jobs also have free CPU/RAM quotas. Firestore has free quota for the default database, but your named database admindfirestore does not qualify for the free quota, although costs should still be tiny at your current scale. Vercel Hobby is intended for personal, non-commercial use, while Pro is for professional developers, freelancers, and businesses, so an internal company DeepWiki is not a clean fit for free Hobby.

Rough estimate for your DeepWiki app on Cloud Run:

Very low internal usage:
  likely $0–$5/month for Cloud Run itself

Firestore:
  likely cents to a few dollars/month at current scale

BigQuery:
  likely very low if queries are small and cached, but avoid querying BigQuery on every page load unnecessarily

Gemini/OpenAI:
  this is the bigger variable, but page loads should not call LLMs if wiki pages are pre-generated

The biggest actual cost risk is Gemini Enterprise subscription/licensing, not Cloud Run hosting.

18. Fix the Next.js prerender issue

Even if you deploy to Cloud Run, fix this in Next.js.

On pages that call BigQuery/Firestore, force runtime rendering:

export const dynamic = "force-dynamic";

Remove or avoid:

export const revalidate = 60;

for pages that need credentials at render time.

Better:

Homepage:
  reads BigQuery/Firestore at request time

Project page:
  reads Firestore wiki at request time

Ask AI:
  API route only, never during static build

If you deploy to Cloud Run, assign the app a service account such as:

deepwiki-webapp@admind-data-organisation.iam.gserviceaccount.com

or reuse project-intelligence-worker for quick testing, then split later.

Recommended roles for the web app:

roles/bigquery.dataViewer
roles/bigquery.jobUser
roles/datastore.user
roles/aiplatform.user

Use a separate web app service account later because the web app does not need Scoro write/sync permissions.

19. What to do next, in order

Do this sequence:

1. Do not cancel the free Gemini subscription. The active assigned licenses are from the trial.
2. Cancel/disable renewal on the paid subscription if you do not need it, because it has 0 assigned licenses.
3. Fix taxonomy design:
   add project_document_candidates table.
4. Change document-discovery:
   write candidates per project.
5. Change taxonomy-sync:
   classify each project only against its own candidates.
6. Increase DOCUMENT_LIMIT from 10 to 50.
7. Improve Discovery Engine payload:
   request extractive answers/segments, not only snippets.
8. Change wiki-generate:
   generate only for projects with mapped documents.
9. Recreate Cloud Scheduler with the commands above.
10. Deploy DeepWiki to Cloud Run, not Vercel/Render, for this internal GCP-backed app.

Your current setup is not broken. It is just at the first working prototype stage. The key fix is to stop treating documents as one global pool and start preserving the relationship:

Project → Discovery Engine candidate documents → LLM-confirmed mappings → Wiki

Once you add project_document_candidates, the taxonomy and wiki quality should improve a lot.

why do you think it only brought only 26 projects even though our scroro has more projects? I would like to fetch all projects, even the completed projects. I would also like to use other data connectors like scoro MCP that are connected but there's no ingestion. also i can see that for existing projects, when i click on that specific file, it opens in another tab and give me no access error from google. how can i approach that? i want all users to use that web app to view all projects and all files
Thought for 44s

Yes, your concern is correct. Right now the system is working, but it is still behaving like a prototype pipeline, not a complete “all company projects and files” platform.

Why it only fetched 26 Scoro projects

Because your current Scoro code explicitly asks Scoro for only projects with this status:

"filter": {"status": "inprogress"}

That is in fetch_scoro_projects() in your worker.

So the 26 projects are not “all Scoro projects.” They are only the projects Scoro returned for:

status = inprogress

That also explains why your DeepWiki UI says “26 active projects.” The pipeline is not fetching completed, cancelled, archived, or other statuses.

Fix: fetch all projects

Change this function:

def fetch_scoro_projects(page: int = 1, per_page: int = 50) -> list[dict]:
    data = _scoro_post("projects/list", {
        "page": page,
        "per_page": per_page,
        "filter": {"status": "inprogress"},
    })
    return data.get("data", [])

to:

def fetch_scoro_projects(page: int = 1, per_page: int = 50) -> list[dict]:
    data = _scoro_post("projects/list", {
        "page": page,
        "per_page": per_page,
    })
    return data.get("data", [])

Then redeploy and run:

gcloud run jobs update admind-taxonomy-worker \
  --region europe-west1 \
  --update-env-vars JOB_TYPE=scoro-sync,PROJECT_LIMIT=500

gcloud run jobs execute admind-taxonomy-worker \
  --region europe-west1 \
  --wait

Then check:

SELECT status, COUNT(*) AS count
FROM `admind-data-organisation.admind_data_organisation.projects`
GROUP BY status
ORDER BY count DESC;

This will tell you which statuses Scoro actually returns.

Important: get_active_projects() also filters out completed projects

Even if you fetch all projects from Scoro, the rest of the pipeline still only processes “active” projects because get_active_projects() excludes completed/closed/cancelled projects. Your code currently does:

WHERE LOWER(CAST(status AS STRING)) NOT IN ('done', 'completed', 'cancelled', 'closed')

That is fine for nightly active-project processing, but not for your goal of showing all projects.

You should split this into two functions:

def get_all_projects(limit: int = 500):
    sql = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.projects`
    ORDER BY start_date DESC
    LIMIT @limit
    """
    return run_query(sql, [bigquery.ScalarQueryParameter("limit", "INT64", limit)])

Keep the current get_active_projects() for daily work, but use get_all_projects() for:

project listing in the web app
historical wiki generation
one-time backfill jobs

Then add a new job type:

historical-full-sync

which processes all projects, not just active ones.

How I would structure Scoro fetching

You want two modes:

Mode 1: daily-sync
Fetch active/in-progress/recently updated projects only.

Mode 2: historical-backfill
Fetch all Scoro projects, including completed ones.

Add env var:

SCORO_PROJECT_MODE=all

Then:

def fetch_scoro_projects(page: int = 1, per_page: int = 50) -> list[dict]:
    body = {
        "page": page,
        "per_page": per_page,
    }

    mode = os.getenv("SCORO_PROJECT_MODE", "active")

    if mode == "active":
        body["filter"] = {"status": "inprogress"}

    data = _scoro_post("projects/list", body)
    return data.get("data", [])

Then for historical import:

gcloud run jobs update admind-taxonomy-worker \
  --region europe-west1 \
  --update-env-vars JOB_TYPE=scoro-sync,SCORO_PROJECT_MODE=all,PROJECT_LIMIT=1000

For daily automation:

gcloud run jobs update admind-taxonomy-worker \
  --region europe-west1 \
  --update-env-vars JOB_TYPE=full-sync,SCORO_PROJECT_MODE=active,PROJECT_LIMIT=50,DOCUMENT_LIMIT=50
About Scoro MCP

The Scoro MCP connector you see in Gemini Enterprise is not the same thing as ingestion into your BigQuery taxonomy.

There are two different concepts:

Gemini Enterprise connector / MCP
  Lets Gemini Enterprise search or interact with a tool inside its own environment.

Your Cloud Run taxonomy pipeline
  Needs structured data written into BigQuery.

So even if Scoro MCP is connected in Gemini Enterprise, your BigQuery will not automatically receive Scoro projects unless your worker explicitly calls Scoro and writes to BigQuery.

For your use case, I would not rely on Scoro MCP as the source of truth for taxonomy. Use:

Scoro API → Cloud Run → BigQuery.projects

Then optionally use MCP later for agent actions like:

“Create a Scoro task”
“Update project status”
“Fetch extra project detail”

But for reliable nightly ingestion, the API is better than agent/MCP search.

Why file links open with “no access”

This is expected.

Your documents.url is often something like:

gs://discoveryengine-third-party-connector-...

That is not the original Google Drive link. It is an internal Cloud Storage location used by Discovery Engine’s connector indexing pipeline. Your current code stores:

"url": derived.get("link") or struct.get("uri") or struct.get("url") or ""

and in many cases Discovery Engine is giving you an internal gs://... URI.

So when a user clicks it, one of two things happens:

1. It is not a user-facing Drive URL.
2. The user does not have permission to the original source file.

The Google access error is not a bug in your app. It is an access-control issue.

How to let all users view all project files

You have three possible approaches.

Option A: Respect original file permissions

This is the safest enterprise approach.

Each user signs into DeepWiki with Google/Microsoft SSO. The app shows project wiki content, but when the user clicks a file, the original Google Drive/SharePoint permission still applies.

Pros:

Secure
Respects client confidentiality
No accidental data leakage

Cons:

Some users see “no access”

This is the right approach if Admind has sensitive client work.

Option B: Give all users access to a shared archive

Move/copy final project files into a controlled Shared Drive, for example:

Shared Drive: Admind Project Knowledge Base
Group access: deepwiki-users@admindagency.com

Then all DeepWiki users can open files.

Pros:

Simple for users
Works with Google Drive permissions
Good for final deliverables and approved docs

Cons:

Requires document governance
May duplicate content
Must avoid exposing restricted client files

This is probably the best operational model for Admind.

Option C: Proxy file content through your app

Your backend uses a privileged service account or delegated user to fetch file contents and display them inside DeepWiki.

Pros:

Users can view everything inside one app
No Drive access errors

Cons:

Highest security risk
You bypass normal Drive sharing behavior
Requires strong authorization rules
Requires audit logs
Potential client confidentiality issue

I do not recommend this as the default. Only use it for approved content, and only after legal/security approval.

The right file access model for your project

I recommend this:

1. DeepWiki shows generated summaries to all authorized internal users.
2. DeepWiki shows source file titles for transparency.
3. Original file links open only if the user has permission.
4. For files everyone should see, create an approved shared archive folder.
5. Add source_url to BigQuery only for approved/shareable files.

Add these fields to documents:

ALTER TABLE `admind-data-organisation.admind_data_organisation.documents`
ADD COLUMN IF NOT EXISTS source_url STRING;

ALTER TABLE `admind-data-organisation.admind_data_organisation.documents`
ADD COLUMN IF NOT EXISTS access_level STRING;

ALTER TABLE `admind-data-organisation.admind_data_organisation.documents`
ADD COLUMN IF NOT EXISTS source_acl_status STRING;

Example values:

source_url:
  original Google Drive / SharePoint / Frame.io / Figma URL

access_level:
  inherited_source_permissions
  company_visible
  restricted
  archived_public_internal

source_acl_status:
  user_must_request_access
  group_access_granted
  unknown

Then your UI can say:

Open source file
Request access
Internal archive copy
Restricted source
How to improve the file links from Discovery Engine

Discovery Engine search is giving you document metadata and snippets, but your parser is not guaranteed to extract the original Drive link. You need to inspect the raw result.

Temporarily log the raw Discovery Engine document for one result:

logger.info("RAW_DISCOVERY_DOC=%s", json.dumps(doc, ensure_ascii=False)[:5000])

Then run document-discovery for one project and inspect Cloud Logging.

Look for fields like:

link
uri
url
source_uri
drive_id
mime_type
structData
derivedStructData

Then update parsing:

source_url = (
    derived.get("link")
    or derived.get("uri")
    or struct.get("link")
    or struct.get("uri")
    or struct.get("url")
    or struct.get("source_url")
    or ""
)

Store both:

"url": raw_internal_url,
"source_url": source_url,

Do not overwrite internal gs:// if it is useful for debugging, but do not show it as the clickable user link.

To fetch all projects and files, do this in stages
Stage 1: Backfill all Scoro projects

Change Scoro fetch to remove the inprogress filter.

Deploy.

Run:

gcloud run jobs update admind-taxonomy-worker \
  --region europe-west1 \
  --update-env-vars JOB_TYPE=scoro-sync,SCORO_PROJECT_MODE=all,PROJECT_LIMIT=1000

gcloud run jobs execute admind-taxonomy-worker \
  --region europe-west1 \
  --wait

Check:

SELECT status, COUNT(*) AS count
FROM `admind-data-organisation.admind_data_organisation.projects`
GROUP BY status
ORDER BY count DESC;
Stage 2: Backfill document discovery for all projects

Do not process all projects in one go at first. Start with 100.

gcloud run jobs update admind-taxonomy-worker \
  --region europe-west1 \
  --update-env-vars JOB_TYPE=document-discovery,PROJECT_MODE=all,PROJECT_LIMIT=100,DOCUMENT_LIMIT=20

gcloud run jobs execute admind-taxonomy-worker \
  --region europe-west1 \
  --wait

But your code must use get_all_projects() when PROJECT_MODE=all.

Stage 3: Create project-document candidates

As discussed earlier, add:

project_document_candidates

Because right now your discovery finds documents per project, but taxonomy later classifies against a global random document pool. That is the biggest reason the wiki is weak.

Stage 4: Generate wiki only for projects with mapped docs

Do not create “wiki available” pages with zero source docs. It confuses users.

Automated workflow

You should have two scheduler jobs, not one.

Scheduler 1: Daily active sync

Runs every night.

JOB_TYPE=full-sync
SCORO_PROJECT_MODE=active
PROJECT_MODE=active
PROJECT_LIMIT=50
DOCUMENT_LIMIT=50

Purpose:

Keep active projects fresh.
Scheduler 2: Weekly historical backfill

Runs once per week or manually.

JOB_TYPE=historical-full-sync
SCORO_PROJECT_MODE=all
PROJECT_MODE=all
PROJECT_LIMIT=500
DOCUMENT_LIMIT=20

Purpose:

Slowly enrich old completed projects.

Cloud Run Jobs can be triggered on a schedule using Cloud Scheduler, which is the right mechanism here.

Commands to create a clean nightly scheduler

First update the job to the default nightly behavior:

gcloud run jobs update admind-taxonomy-worker \
  --region europe-west1 \
  --update-env-vars JOB_TYPE=full-sync,SCORO_PROJECT_MODE=active,PROJECT_MODE=active,PROJECT_LIMIT=50,DOCUMENT_LIMIT=50

Grant invoke permission:

gcloud run jobs add-iam-policy-binding admind-taxonomy-worker \
  --region europe-west1 \
  --member="serviceAccount:project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com" \
  --role="roles/run.invoker"

Delete the old broken scheduler if it exists:

gcloud scheduler jobs list --location europe-west1

Then:

gcloud scheduler jobs delete admind-taxonomy-worker-nightly \
  --location europe-west1

Create a new one:

gcloud scheduler jobs create http admind-taxonomy-worker-nightly \
  --location europe-west1 \
  --schedule "0 2 * * *" \
  --time-zone "Europe/Warsaw" \
  --uri "https://run.googleapis.com/v2/projects/admind-data-organisation/locations/europe-west1/jobs/admind-taxonomy-worker:run" \
  --http-method POST \
  --oauth-service-account-email project-intelligence-worker@admind-data-organisation.iam.gserviceaccount.com

Test it:

gcloud scheduler jobs run admind-taxonomy-worker-nightly \
  --location europe-west1

Then verify:

gcloud run jobs executions list \
  --job admind-taxonomy-worker \
  --region europe-west1
Summary

It fetched only 26 projects because your code explicitly filters Scoro to status = inprogress. Remove that filter or make it configurable.

For Scoro MCP, do not expect it to ingest structured project rows into BigQuery automatically. Use the Scoro API for BigQuery taxonomy, and use MCP later for agent actions.

For file access, the current gs://... links are internal Discovery Engine artifact links, not user-friendly Drive links. Even with proper links, users need permission to the underlying files. The cleanest enterprise approach is to use a Shared Drive or Google Group for approved project files, while keeping restricted files behind original permissions.

Your next technical fix should be:

Fetch all Scoro projects → store project-specific Discovery candidates → classify per project → generate wiki only when mapped docs exist → use real source_url links with a clear access policy.

