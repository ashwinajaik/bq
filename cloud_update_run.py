# -*- coding: utf-8 -*-

import bigquery_demo
import sentiment_analysis

success = True
if (success == True):
    success = bigquery_demo.main_run(success)
    print('All job runs completed successfully')
if (success == True):
    success = sentiment_analysis.main_run(success)

    