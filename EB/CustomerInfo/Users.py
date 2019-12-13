from abc import ABC, abstractmethod
from Context.Context import Context
from DataAccess.DataObject import UsersRDB as UsersRDB
# The base classes would not be IN the project. They would be in a separate included package.
# They would also do some things.
import boto3
import json


class ServiceException(Exception):

    unknown_error   =   9001
    missing_field   =   9002
    bad_data        =   9003

    def __init__(self, code=unknown_error, msg="Oh Dear!"):
        self.code = code
        self.msg = msg


class BaseService():

    missing_field = 2001

    def __init__(self):
        pass


class UsersService(BaseService):

    required_create_fields = ['last_name', 'first_name', 'email', 'password']

    def __init__(self, ctx=None):

        if ctx is None:
            ctx = Context.get_default_context()
        self.sns_client = boto3.client('sns',region_name='us-east-1')
        self._ctx = ctx


    @classmethod
    def get_by_email(cls, email):

        result = UsersRDB.get_by_email(email)
        return result

    @classmethod
    def create_user(cls, user_info):
        for f in UsersService.required_create_fields:
            v = user_info.get(f, None)
            if v is None:
                raise ServiceException(ServiceException.missing_field,
                                       "Missing field = " + f)

            if f == 'email':
                if v.find('@') == -1:
                    raise ServiceException(ServiceException.bad_data, "Email looks invalid: " + v)

        result = UsersRDB.create_user(user_info=user_info)
        email = user_info.get('email', None)
        response = boto3.client('sns',region_name='us-east-1').publish(
            TopicArn='arn:aws:sns:us-east-1:270598185649:user_created',    
            Message = json.dumps({'customers_email' : email }),    
        )

#         # Print out the response
#         print(response)
        return result

    '''
    Delete existing user information

    :param user_email: Email id of the user to be deleted
    :return Boolean
    '''
    @classmethod
    def delete_user(cls, user_email):
        if UsersRDB.get_by_email(user_email) is None:
            raise TypeError("User with given email: " + user_email + " does not exist.")

        result = UsersRDB.delete_user(user_email)
        return result

    '''
    Update existing user information

    :param user_email: Email id of the user to be updated
    :param new_user_info: Dictionary containing new user information

    :return Boolean
    '''
    @classmethod
    def update_user(cls, user_email, new_user_info):
        if UsersRDB.get_by_email(user_email) is None:
            raise TypeError("User with given email: " + user_email + " does not exist.")

        result = UsersRDB.update_user(user_email, new_user_info)
        return result