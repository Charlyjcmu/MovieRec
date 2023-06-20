import urllib.parse

uid_encoded = "%7Buid%7D"
uid_decoded = urllib.parse.unquote(uid_encoded)
print(uid_decoded)
#uid_number = int(uid_decoded)
#print(uid_number)