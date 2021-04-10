import json
from requests import Response
import logging
from typing import Union

class ResponseTreat:
    HTTP_CREATED = 201
    HTTP_SUCCESS = 200
    HTTP_ERROR = 500

    def treatment(self, response: Response,
                  pretty_response: bool = True) -> Union[dict, str]:
        """
        description: This method is responsible to return an indented json file
        or a dict.

        response: file that will be indented.

        return: Indented json file or a dict.
        """
        if response.status_code >= self.HTTP_ERROR:
            logging.error(response.text)
        elif (
                response.status_code != self.HTTP_SUCCESS
                and response.status_code != self.HTTP_CREATED
        ):
            logging.warning(response.json()["result"])
            return {}
        else:
            if pretty_response:
                return json.dumps(response.json(), indent=4, sort_keys=True)
            else:
                return response.json()
