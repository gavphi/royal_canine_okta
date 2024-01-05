from api_functions.okta import create_user, update_user, get_user

"""res = create_user(
    {"firstName": "Alexandra",
    "lastName": "Marilia",
    "mobilePhone": "000000000",
    "email": "alexandratkf@gmail.com",
    "login": "alexandratkf@gmail.com",
    "preferredLanguage": "Spanish",
    "countryCode": "311",}

)


print(res.text)"""

"""
res = update_user(
    "00u1xi5rv44tVqOeg0h8"
    "Marilia",
    "Soares",
    "000000200",
    "testingmario@gmail.com",
    "testingmario@gmail.com",
    "Spanish",
    "311",
    "test",
)

print(res.text)

"""

res = get_user("00u1xi5rv44tVqOeg0h8")
print(res.text)
