from bingads import OAuthTokenRequestException
from bingads.service_client import ServiceClient
from bingads.authorization import AuthorizationData, OAuthDesktopMobileAuthCodeGrant

import sys
import webbrowser

from bingads.v13.reporting import ReportingServiceManager
from past.builtins import raw_input
from time import gmtime, strftime
from suds import WebFault

# Required
from job import build_bing_report_request, download_campaign_report

CLIENT_ID = 'd2dad530-e0d5-40bf-ba44-19f5d84494f2'
CLIENT_SECRET = 'eQs8Q~L1Tw-nj4aNa1PIOnL087i~qfXYiRM1ibdW'
DEVELOPER_TOKEN='1298EB0EEF671723'
ENVIRONMENT='production'
REFRESH_TOKEN="M.R3_SN1.-CUrob3USkHSU9xf76YseHb!8Rvok01qBsQYweLjsZT*0NHyPGMzwKPJbrYTYWCDriAIB9hT8NiqC7v*DqJ1DBMVRYwu*FsR1klLzz1Qq4YUViPjlYP9W0UI3MEfxETZl2!GgJLIlyOfmId5y1nOAGEAc*oyN7BkFfIojI9Aqd5clAWq9tIKrdndAynWF5ICnvYjMifRrfdcmS!ZS5KoxK97bkahezFnsu05IRMH!0nXWhxcXp6IwzlzD1zf6BrIlL65AEmfobVcRE3dsj9fdXVT*clLC2JCku55760vBbUMC4vTH1ZsQy!6NeSp23VU!tdmY9CutE6z4*xbkVvFNlQzZOCaBQfK5yFGfWFMYWiv3DVdUIQFqN8v2UBP!OdwMS0BL9cfY0y5xUMc806P2RnSCNM!mMYwBHupi!MSFlxtrK*prT4vuTb*!P4*nNTQfew$$"

# Optional
CLIENT_STATE='ClientStateGoesHere'

# Optionally you can include logging to output traffic, for example the SOAP request and response.
# import logging
# logging.basicConfig(level=logging.INFO)
# logging.getLogger('suds.client').setLevel(logging.DEBUG)
# logging.getLogger('suds.transport.http').setLevel(logging.DEBUG)

def authenticate(authorization_data):

    customer_service=ServiceClient(
        service='CustomerManagementService',
        version=13,
        authorization_data=authorization_data,
        environment=ENVIRONMENT,
    )

    # You should authenticate for Bing Ads API service operations with a Microsoft Account.
    authenticate_with_oauth(authorization_data)

    # Set to an empty user identifier to get the current authenticated Microsoft Advertising user,
    # and then search for all accounts the user can access.
    user=get_user_response=customer_service.GetUser(
        UserId=None
    ).User
    accounts=search_accounts_by_user_id(customer_service, user.Id)

    # For this example we'll use the first account.
    authorization_data.account_id=accounts['AdvertiserAccount'][0].Id
    authorization_data.customer_id=accounts['AdvertiserAccount'][0].ParentCustomerId

def authenticate_with_oauth(authorization_data):

    # class OAuthDesktopMobileAuthCodeGrantWithSecret(OAuthDesktopMobileAuthCodeGrant):
    #     def __init__(self, *args, client_secret=None, **kwargs):
    #         super().__init__(*args, **kwargs)
    #         self._client_secret = client_secret

    authentication=OAuthDesktopMobileAuthCodeGrant(
    # authentication=OAuthDesktopMobileAuthCodeGrantWithSecret(
        client_id=CLIENT_ID,
        # client_secret=CLIENT_SECRET,
        env=ENVIRONMENT
    )

    authentication.client_secret = CLIENT_SECRET

    # It is recommended that you specify a non guessable 'state' request parameter to help prevent
    # cross site request forgery (CSRF).
    authentication.state=CLIENT_STATE

    # Assign this authentication instance to the authorization_data.
    authorization_data.authentication=authentication

    # Register the callback function to automatically save the refresh token anytime it is refreshed.
    # Uncomment this line if you want to store your refresh token. Be sure to save your refresh token securely.
    authorization_data.authentication.token_refreshed_callback=save_refresh_token

    refresh_token=get_refresh_token()

    try:
        # If we have a refresh token let's refresh it
        if refresh_token is not None:
            authorization_data.authentication.request_oauth_tokens_by_refresh_token(refresh_token)
        else:
            request_user_consent(authorization_data)
    except OAuthTokenRequestException:
        # The user could not be authenticated or the grant is expired.
        # The user must first sign in and if needed grant the client application access to the requested scope.
        request_user_consent(authorization_data)

def request_user_consent(authorization_data):
    webbrowser.open(authorization_data.authentication.get_authorization_endpoint(), new=1)
    # For Python 3.x use 'input' instead of 'raw_input'
    if(sys.version_info.major >= 3):
        response_uri=input(
            "You need to provide consent for the application to access your Microsoft Advertising accounts. " \
            "After you have granted consent in the web browser for the application to access your Microsoft Advertising accounts, " \
            "please enter the response URI that includes the authorization 'code' parameter: \n"
        )
    else:
        response_uri=raw_input(
            "You need to provide consent for the application to access your Microsoft Advertising accounts. " \
            "After you have granted consent in the web browser for the application to access your Microsoft Advertising accounts, " \
            "please enter the response URI that includes the authorization 'code' parameter: \n"
        )

    if authorization_data.authentication.state != CLIENT_STATE:
        raise Exception("The OAuth response state does not match the client request state.")

    # Request access and refresh tokens using the URI that you provided manually during program execution.
    authorization_data.authentication.request_oauth_tokens_by_response_uri(response_uri=response_uri)

def get_refresh_token():
    '''
    Returns a refresh token if found.
    '''
    return REFRESH_TOKEN

def save_refresh_token(oauth_tokens):
    '''
    Stores a refresh token locally. Be sure to save your refresh token securely.
    '''
    return None

def search_accounts_by_user_id(customer_service, user_id):
    predicates={
        'Predicate': [
            {
                'Field': 'UserId',
                'Operator': 'Equals',
                'Value': user_id,
            },
        ]
    }

    accounts=[]

    page_index = 0
    PAGE_SIZE=100
    found_last_page = False

    while (not found_last_page):
        paging=set_elements_to_none(customer_service.factory.create('ns5:Paging'))
        paging.Index=page_index
        paging.Size=PAGE_SIZE
        search_accounts_response = customer_service.SearchAccounts(
            PageInfo=paging,
            Predicates=predicates
        )

        if search_accounts_response is not None and hasattr(search_accounts_response, 'AdvertiserAccount'):
            accounts.extend(search_accounts_response['AdvertiserAccount'])
            found_last_page = PAGE_SIZE > len(search_accounts_response['AdvertiserAccount'])
            page_index += 1
        else:
            found_last_page=True

    return {
        'AdvertiserAccount': accounts
    }

def set_elements_to_none(suds_object):
    for (element) in suds_object:
        suds_object.__setitem__(element[0], None)
    return suds_object

def output_status_message(message):
    print(message)

def output_bing_ads_webfault_error(error):
    if hasattr(error, 'ErrorCode'):
        output_status_message("ErrorCode: {0}".format(error.ErrorCode))
    if hasattr(error, 'Code'):
        output_status_message("Code: {0}".format(error.Code))
    if hasattr(error, 'Details'):
        output_status_message("Details: {0}".format(error.Details))
    if hasattr(error, 'FieldPath'):
        output_status_message("FieldPath: {0}".format(error.FieldPath))
    if hasattr(error, 'Message'):
        output_status_message("Message: {0}".format(error.Message))
    output_status_message('')

def output_webfault_errors(ex):
    if not hasattr(ex.fault, "detail"):
        raise Exception("Unknown WebFault")

    error_attribute_sets = (
        ["ApiFault", "OperationErrors", "OperationError"],
        ["AdApiFaultDetail", "Errors", "AdApiError"],
        ["ApiFaultDetail", "BatchErrors", "BatchError"],
        ["ApiFaultDetail", "OperationErrors", "OperationError"],
        ["EditorialApiFaultDetail", "BatchErrors", "BatchError"],
        ["EditorialApiFaultDetail", "EditorialErrors", "EditorialError"],
        ["EditorialApiFaultDetail", "OperationErrors", "OperationError"],
    )

    for error_attribute_set in error_attribute_sets:
        if output_error_detail(ex.fault.detail, error_attribute_set):
            return

    # Handle serialization errors, for example: The formatter threw an exception while trying to deserialize the message, etc.
    if hasattr(ex.fault, 'detail') \
        and hasattr(ex.fault.detail, 'ExceptionDetail'):
        api_errors=ex.fault.detail.ExceptionDetail
        if isinstance(api_errors, list):
            for api_error in api_errors:
                output_status_message(api_error.Message)
        else:
            output_status_message(api_errors.Message)
        return

    raise Exception("Unknown WebFault")

def output_error_detail(error_detail, error_attribute_set):
    api_errors = error_detail
    for field in error_attribute_set:
        api_errors = getattr(api_errors, field, None)
    if api_errors is None:
        return False
    if isinstance(api_errors, list):
        for api_error in api_errors:
            output_bing_ads_webfault_error(api_error)
    else:
        output_bing_ads_webfault_error(api_errors)
    return True

def output_array_of_long(items):
    if items is None or items['long'] is None:
        return
    output_status_message("Array Of long:")
    for item in items['long']:
        output_status_message("{0}".format(item))

def output_customerrole(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_customerrole * * *")
    output_status_message("RoleId: {0}".format(data_object.RoleId))
    output_status_message("CustomerId: {0}".format(data_object.CustomerId))
    output_status_message("AccountIds:")
    output_array_of_long(data_object.AccountIds)
    output_status_message("LinkedAccountIds:")
    output_array_of_long(data_object.LinkedAccountIds)
    output_status_message("* * * End output_customerrole * * *")

def output_array_of_customerrole(data_objects):
    if data_objects is None or len(data_objects) == 0:
        return
    for data_object in data_objects['CustomerRole']:
        output_customerrole(data_object)

def output_keyvaluepairofstringstring(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_keyvaluepairofstringstring * * *")
    output_status_message("key: {0}".format(data_object.key))
    output_status_message("value: {0}".format(data_object.value))
    output_status_message("* * * End output_keyvaluepairofstringstring * * *")

def output_array_of_keyvaluepairofstringstring(data_objects):
    if data_objects is None or len(data_objects) == 0:
        return
    for data_object in data_objects['KeyValuePairOfstringstring']:
        output_keyvaluepairofstringstring(data_object)

def output_personname(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_personname * * *")
    output_status_message("FirstName: {0}".format(data_object.FirstName))
    output_status_message("LastName: {0}".format(data_object.LastName))
    output_status_message("MiddleInitial: {0}".format(data_object.MiddleInitial))
    output_status_message("* * * End output_personname * * *")

def output_address(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_address * * *")
    output_status_message("City: {0}".format(data_object.City))
    output_status_message("CountryCode: {0}".format(data_object.CountryCode))
    output_status_message("Id: {0}".format(data_object.Id))
    output_status_message("Line1: {0}".format(data_object.Line1))
    output_status_message("Line2: {0}".format(data_object.Line2))
    output_status_message("Line3: {0}".format(data_object.Line3))
    output_status_message("Line4: {0}".format(data_object.Line4))
    output_status_message("PostalCode: {0}".format(data_object.PostalCode))
    output_status_message("StateOrProvince: {0}".format(data_object.StateOrProvince))
    output_status_message("TimeStamp: {0}".format(data_object.TimeStamp))
    output_status_message("BusinessName: {0}".format(data_object.BusinessName))
    output_status_message("* * * End output_address * * *")

def output_contactinfo(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_contactinfo * * *")
    output_status_message("Address:")
    output_address(data_object.Address)
    output_status_message("ContactByPhone: {0}".format(data_object.ContactByPhone))
    output_status_message("ContactByPostalMail: {0}".format(data_object.ContactByPostalMail))
    output_status_message("Email: {0}".format(data_object.Email))
    output_status_message("EmailFormat: {0}".format(data_object.EmailFormat))
    output_status_message("Fax: {0}".format(data_object.Fax))
    output_status_message("HomePhone: {0}".format(data_object.HomePhone))
    output_status_message("Id: {0}".format(data_object.Id))
    output_status_message("Mobile: {0}".format(data_object.Mobile))
    output_status_message("Phone1: {0}".format(data_object.Phone1))
    output_status_message("Phone2: {0}".format(data_object.Phone2))
    output_status_message("* * * End output_contactinfo * * *")

def output_user(data_object):
    if data_object is None:
        return
    output_status_message("* * * Begin output_user * * *")
    output_status_message("ContactInfo:")
    output_contactinfo(data_object.ContactInfo)
    output_status_message("CustomerId: {0}".format(data_object.CustomerId))
    output_status_message("Id: {0}".format(data_object.Id))
    output_status_message("JobTitle: {0}".format(data_object.JobTitle))
    output_status_message("LastModifiedByUserId: {0}".format(data_object.LastModifiedByUserId))
    output_status_message("LastModifiedTime: {0}".format(data_object.LastModifiedTime))
    output_status_message("Lcid: {0}".format(data_object.Lcid))
    output_status_message("Name:")
    output_personname(data_object.Name)
    output_status_message("Password: {0}".format(data_object.Password))
    output_status_message("SecretAnswer: {0}".format(data_object.SecretAnswer))
    output_status_message("SecretQuestion: {0}".format(data_object.SecretQuestion))
    output_status_message("UserLifeCycleStatus: {0}".format(data_object.UserLifeCycleStatus))
    output_status_message("TimeStamp: {0}".format(data_object.TimeStamp))
    output_status_message("UserName: {0}".format(data_object.UserName))
    output_status_message("ForwardCompatibilityMap:")
    output_array_of_keyvaluepairofstringstring(data_object.ForwardCompatibilityMap)
    output_status_message("* * * End output_user * * *")

def main(authorization_data):

    try:
        output_status_message("-----\nGetUser:")
        get_user_response=customer_service.GetUser(
            UserId=None
        )
        user = get_user_response.User
        customer_roles=get_user_response.CustomerRoles
        output_status_message("User:")
        output_user(user)
        output_status_message("CustomerRoles:")
        output_array_of_customerrole(customer_roles)

        # Search for the accounts that the user can access.
        # To retrieve more than 100 accounts, increase the page size up to 1,000.
        # To retrieve more than 1,000 accounts you'll need to add paging.

        accounts=search_accounts_by_user_id(customer_service, user.Id)

        customer_ids=[]
        for account in accounts['AdvertiserAccount']:
            customer_ids.append(account.ParentCustomerId)
        distinct_customer_ids = {'long': list(set(customer_ids))[:100]}

        for customer_id in distinct_customer_ids['long']:
            # You can find out which pilot features the customer is able to use.
            # Each account could belong to a different customer, so use the customer ID in each account.
            output_status_message("-----\nGetCustomerPilotFeatures:")
            output_status_message("Requested by CustomerId: {0}".format(customer_id))
            feature_pilot_flags=customer_service.GetCustomerPilotFeatures(
                CustomerId=customer_id
            )
            output_status_message("Customer Pilot flags:")
            output_status_message("; ".join(str(flag) for flag in feature_pilot_flags['int']))

    except WebFault as ex:
        output_webfault_errors(ex)
    except Exception as ex:
        output_status_message(ex)

# Main execution
if __name__ == '__main__':
    BING_ENVIRONMENT = 'production'
    API_VERSION = 13

    print("Loading the web service client proxies...")

    authorization_data=AuthorizationData(
        account_id=None,
        customer_id=None,
        developer_token=DEVELOPER_TOKEN,
        authentication=None,
    )

    customer_service=ServiceClient(
        service='CustomerManagementService',
        version=13,
        authorization_data=authorization_data,
        environment=ENVIRONMENT,
    )

    reporting_service_manager = ReportingServiceManager(
        authorization_data=authorization_data,
        poll_interval_in_milliseconds=5000,
        environment=BING_ENVIRONMENT,
    )

    reporting_service = ServiceClient(
        service='ReportingService',
        authorization_data=authorization_data,
        environment=BING_ENVIRONMENT,
        version=API_VERSION,
    )

    authenticate(authorization_data)

    # main(authorization_data)

    report_request = build_bing_report_request(reporting_service, authorization_data)
    report_data = download_campaign_report(report_request, authorization_data)

    print(report_data)