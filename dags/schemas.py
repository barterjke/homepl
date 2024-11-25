import os.path


def get_contacts_schema(file_path: str) -> dict:
    file_name = os.path.basename(file_path)
    return {
        "name": "November Marketing Event Leads",
        "dateFormat": "DAY_MONTH_YEAR",
        "files": [
            {
                "fileName": file_name,  # should match exactly, or else 400
                "fileFormat": "CSV",
                "fileImportPage": {
                    "hasHeader": True,  # firstname,lastname,email
                    "columnMappings": [
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "First Name",
                            "propertyName": "firstname"
                        },
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "Last Name",
                            "propertyName": "lastname"
                        },
                        {
                            "columnObjectTypeId": "0-1",
                            "columnName": "Email",
                            "propertyName": "email",
                            "associationIdentifierColumn": True
                        }
                    ]
                }
            }
        ]
    }