import requests
import json
import pydash
import string

import pandas as pd

from collections import Counter
from unidecode import unidecode
from nltk import ngrams

from typing import Tuple

from utils.logger import get_logger
from utils.exc import EndpointException

from pdb import set_trace as bp

logger = get_logger(__name__)


class SWGOH(object):
    def __init__(self) -> None:
        self.swgoh_api = "http://swgoh.gg/api/"

    # region aux functions
    def __read_api(self, endpoint: str) -> dict:
        try:
            ep = f"{self.swgoh_api}{endpoint}"

            response = requests.get(ep)
            if response.status_code == 200:
                res = json.loads(response.content)
                return {"ok": 1,
                        "content": res}
            else:
                message = f"An error ocurred while reading the endpoint {ep}: Status code: {response.status_code}"
                logger.error(message)
                return {"ok": 0,
                        "code": response.status_code,
                        "message": message,
                        "content": {}}

        except Exception as ex:
            logger.error(ex)
            raise ex

    def __create_char_aliases(self, chars: list):
        try:
            alias_ngrams = Counter()
            res_chars = {}

            def plain_name(name):
                name = unidecode(name)
                return name.translate(str.maketrans('', '', string.punctuation))

            # Building 1-3 grams for aliases
            for n in [1, 2, 3]:
                temp = pydash.chain(chars)\
                    .map(lambda x: plain_name(x["name"]).split())\
                    .map(lambda x: [*ngrams(x, n)])\
                    .flatten()\
                    .map(lambda x: " ".join(x))\
                    .value()

                alias_ngrams.update(temp)

            uniques = set(
                [value for value, count in alias_ngrams.items() if count == 1])

            for char in chars:
                id = char["id"]
                name = plain_name(char["name"]).split()

                aliases = []
                for n in [1, 2, 3]:
                    aliases += [" ".join(r) for r in [*ngrams(name, n)]]

                aliases = set(aliases).intersection(uniques)

                if len(aliases) == 0:
                    aliases = set([plain_name(char["name"])])

                res_chars[id] = {"name": char["name"],
                                 "id": char["id"],
                                 "aliases": aliases,
                                 "base_id": char["base_id"]}

            return res_chars
        except Exception as ex:
            logger.error(ex)
            raise
    # endregion

    # region externally used

    def get_chars(self) -> Tuple[pd.DataFrame, dict]:
        try:
            response = self.__read_api("characters")
            # We have char from here with
            # [{id:str, name:str, base_id:str (This is for searching in some eps)}]
            if response["ok"]:
                response = response["content"]

                chars = pydash.chain(response)\
                    .map(lambda x: {"id": x["pk"],
                                    "name": x["name"].lower(),
                                    "base_id": x["base_id"]})\
                    .value()

                # We convert to
                # {id: {name:str, id:int, aliases:set(), base_id:str}}
                id_chars = self.__create_char_aliases(chars)

                # And finally
                # {alias: {name:str, id:int, aliases:set(), base_id:str}
                alias_chars = {}
                for char in id_chars.values():
                    temp = {
                        alias: {
                            "alias": alias,
                            "id": char["id"],
                            "name": char["name"],
                            "base_id": char["base_id"]
                        }
                        for alias
                        in char["aliases"]
                    }

                    alias_chars.update(temp)

                df_alias = pd.DataFrame(alias_chars.values())

                return df_alias, alias_chars
            else:
                raise EndpointException(
                    self.swgoh_api + "/characters",
                    response["message"],
                    f"**Mr.Lobot** found an error while getting the unit stats")
        except Exception as ex:
            logger.error(ex)
            raise ex

    def guild_info(self, guild: str) -> bool:
        try:
            endpoint = f"guild/{guild}"

            response = self.__read_api(endpoint)
            if response["ok"]:
                if "data" in response["content"]:
                    return response["content"]["data"]
                else:
                    raise EndpointException(
                        f"{self.swgoh_api}/guild/{guild}",
                        "Guild not found content \{\}",
                        f"**Mr.Lobot** cannot find the guild with id **{guild}**. Are sure it exists?")
            else:
                raise EndpointException(
                    f"{self.swgoh_api}/guild/{guild}",
                    response["message"],
                    f"**Mr.Lobot** cannot find the guild with id **{guild}**. Are sure it exists?")
        except Exception as ex:
            logger.error(ex)
            raise

    def get_guild_players(self, guild: str) -> pd.DataFrame:
        try:
            response = self.__read_api(f"guild/{guild}")
            if response["ok"]:
                guild_info = response["content"]
                players = guild_info["players"]

                return players
            else:
                raise EndpointException(
                    f"{self.swgoh_api}/guild/{guild}",
                    response["message"],
                    f"**Mr.Lobot** cannot find any data for the player of the guild **{guild}**")
        except Exception as ex:
            logger.error(ex)
            raise

    # endregion
