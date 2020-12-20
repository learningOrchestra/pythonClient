__author__ = "Otavio Henrique Rodrigues Mapa & Matheus Goncalves Ribeiro"
__credits__ = "all free source developers"
__status__ = "Prototype"

import json


class ResponseTreat:
    HTTP_CREATED = 201
    HTTP_SUCCESS = 200
    HTTP_ERROR = 500

    def treatment(self, response, pretty_response: bool = True):
        """
        description: This method is responsible to return an indented json file
        or a dict.

        response: file that will be indented.

        return: Indented json file or a dict.
        """
        if response.status_code >= self.HTTP_ERROR:
            return response.text
        elif (
                response.status_code != self.HTTP_SUCCESS
                and response.status_code != self.HTTP_CREATED
        ):
            raise Exception(response.json()["result"])
        else:
            if pretty_response:
                return json.dumps(response.json(), indent=4, sort_keys=True)
            else:
                return response.json()
