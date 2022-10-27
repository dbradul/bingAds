import sys
from datetime import datetime, timedelta

from bingads.v13.reporting import ReportingDownloadParameters, ReportingServiceManager


def build_bing_report_request(reporting_service, auth_object):
        '''
        Build a bing report request, including Format, ReportName, Aggregation,
        Scope, Time, Filter, and Columns.
        '''
        report_request=reporting_service.factory.create('AdGroupPerformanceReportRequest')
        report_request.Format='Csv'
        report_request.ReportName='Custom API Report'
        report_request.ReturnOnlyCompleteData=False
        report_request.Aggregation='Daily'
        #This is stupid, why would I want anything else?
        # report_request.Language='English'

        ######  SCOPE  ##############
        # job_scope = job.bing_report_scope_object
        scope=reporting_service.factory.create('AccountThroughAdGroupReportScope')
        # for s in job_scope.child_elements:
        #     statement = 'scope.' + s.xml_type + '=None'
        #     _logger.info(statement)
        #     exec(statement)
        #     #sample
            # scope.Campaigns=None
        scope.AccountIds={'long': [auth_object.account_id] }
        report_request.Scope=scope

        #############################

        ######  DATE/TIME  ############
        report_time=reporting_service.factory.create('ReportTime')
        # if job.bing_date_type == 'selection':
        #     report_time.PredefinedTime=job.bing_date_selection
        #
        # else:
        date_from_obj = datetime.now().date() - timedelta(days=7)
        date_to_obj = datetime.now().date()
        # date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
        # date_to_obj = datetime.strptime(date_to, '%Y-%m-%d')
        custom_date_range_start=reporting_service.factory.create('Date')
        custom_date_range_start.Day=date_from_obj.day
        custom_date_range_start.Month=date_from_obj.month
        custom_date_range_start.Year=date_from_obj.year
        report_time.CustomDateRangeStart=custom_date_range_start
        custom_date_range_end=reporting_service.factory.create('Date')
        custom_date_range_end.Day=date_to_obj.day
        custom_date_range_end.Month=date_to_obj.month
        custom_date_range_end.Year=date_to_obj.year
        report_time.CustomDateRangeEnd=custom_date_range_end
        report_time.PredefinedTime=None

        report_time.ReportTimeZone.value=reporting_service.factory.create('ReportTimeZone').CentralTimeUSCanada
        report_request.Time=report_time

        #############################

        ########  FILTERS  ###########
        # If you specify a filter, results may differ from data you see in the Bing Ads web application
        #TODO
        # report_filter=reporting_service.factory.create('KeywordPerformanceReportFilter')
        # report_filter.DeviceType=[
        #     'Computer',
        #     'SmartPhone'
        # ]
        # report_request.Filter=report_filter

        ######################

        # Specify the attribute and data report columns.
        # columns_xml = job.bing_report_columns_object
        report_columns=reporting_service.factory.create('ArrayOfAdGroupPerformanceReportColumn')
        # column_xml_name = 'AdGroupPerformanceReportColumn'
        # column_statement = \
        #     'report_columns.'\
        #     + column_xml_name\
        #     + '.append(["AccountName", "AccountId", "AdGroupId", "AdGroupName", "AdDistribution", "AdId", "AdType", "AverageCpc", "AverageCpm", "AveragePosition", "Clicks", "Conversions", "CostPerConversion", "Ctr", "CurrencyCode", "Date", "DeviceType", "Impressions", "Language", "Spend"])'
        # exec(column_statement)
        report_columns.AdGroupPerformanceReportColumn.append([
            "AccountName",
            "AccountId",
            "AdGroupId",
            "AdGroupName",
            "AdDistribution",
            # "AdId",
            # "AdType",
            # "AverageCpc",
            # "AverageCpm",
            "AveragePosition",
            "Clicks",
            "Conversions",
            # "CostPerConversion",
            # "Ctr",
            # "CurrencyCode",
            "TimePeriod",
            # "DeviceType",
            "Impressions",
            # "Language",
            "Spend"
        ])
        #example
#        report_columns.KeywordPerformanceReportColumn.append(self.build_bing_report_columns(cr, uid, job))
        report_request.Columns=report_columns

#         if job.bing_report_sort_object:
#             sort_obj = job.bing_sort_column
#             ######  SORT  ######################
#             # You may optionally sort by any KeywordPerformanceReportColumn, and optionally
#             # specify the maximum number of rows to return in the sorted report.
#             report_sorts=reporting_service.factory.create(sort_obj.xml_group)
#             report_sort=reporting_service.factory.create(sort_obj.xml_type)
#             report_sort.SortColumn=job.bing_sort_column.xml_type
#             report_sort.SortOrder=job.bing_sort_order
#             sort_statement = 'report_sorts.' + sort_obj.xml_type + '.append(report_sort)'
#             exec(sort_statement)
#             #example
# #            report_sorts.KeywordPerformanceReportSort.append(report_sort)
#             report_request.Sort=report_sorts
#             #############################

        # if job.bing_row_limit and job.bing_row_limit > 0:
        # report_request.MaxRows=1000

        return report_request


def download_campaign_report(report_request, authorization_data):
    try:
        # startDate = date_validation(s_date)
        # dt = startDate + timedelta(1)
        # week_number = dt.isocalendar()[1]
        # endDate = date_validation(e_date)

        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,
            result_file_directory="./data/",
            result_file_name="campaign_report.csv",
            overwrite_result_file=True,  # value true if you want to overwrite the same file.
            timeout_in_milliseconds=3600000  # cancel the download after a specified time interval.
        )

        reporting_service_manager = ReportingServiceManager(
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=5000,
            environment='production',
        )

        report_container = reporting_service_manager.download_report(reporting_download_parameters)

        if (report_container == None):
            print("There is no report data for the submitted report request parameters.")
            sys.exit(0)

        campaign_analytics_data = {}

        report_record_iterable = report_container.report_records

        for record in report_record_iterable:
            tmp_dict = {}
            tmp_dict["impressions"] = record.int_value("Impressions")
            tmp_dict["clicks"] = record.int_value("Clicks")
            tmp_dict["cost"] = float(record.value("Spend"))
            # print(float(record.value("Spend")))
            tmp_dict["conversions"] = record.int_value("Conversions")
            tmp_dict["AveragePosition"] = record.value("AveragePosition")
            tmp_dict["campaign_id"] = record.int_value("CampaignId")
            tmp_dict["account_name"] = record.value("AccountName")
            tmp_dict["account_id"] = record.int_value("AccountId")

            campaign_analytics_data = campaign_analytics_data.append(tmp_dict)

        # Be sure to close the report.
        report_container.close()

        return campaign_analytics_data
    except Exception as ex:
        print("Error: %s" % ex)
        print("\nDOWNLOAD_CAMPAIGN_REPORT : processing Failed : ", sys.exc_info())