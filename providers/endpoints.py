import os
import json
from typing import List
import requests

from dotenv import load_dotenv

from utils.logger import get_logger
from utils.exc import EndpointException
from pdb import set_trace as bp

logger = get_logger(__name__)


class MrLobotStorageEndpoint(object):
    def __init__(self) -> None:
        self.endpoint = os.getenv("MRLOBOT_ENDPOINT")

    def register_guild(self, server: str,  guild: str):
        try:
            payload = {
                "server": str(server),
                "guild": str(guild)
            }

            print(payload)

            # endpoint/register -> put
            ep = f"{self.endpoint}register"
            response = requests.put(ep, data=json.dumps(payload))

            if response.status_code == 200:
                content = json.loads(response.content)
                is_ok = content.get("ok", False)

                error_type = content.get("errorType")

                if error_type:

                    if error_type == "ConditionalCheckFailedException":
                        raise EndpointException(
                            ep,
                            content["errorMessage"],
                            f"**Mr.Lobot** cannot add more than one guild per server.",
                            payload)
                    else:
                        raise EndpointException(
                            ep,
                            content["errorMessage"],
                            f"**Mr.Lobot** cannot register this guild.",
                            payload)
                elif not is_ok:
                    raise EndpointException(
                        ep,
                        content["message"],
                        content["message"],
                        payload)
                else:
                    return {
                        "ok": 1
                    }
            else:
                raise Exception(f"**Mr.Lobot** found a problem while "
                                f"registering the guild:"
                                f"Status **{response.status_code}**")

        except Exception as ex:
            logger.error(ex)
            raise

    def guild_servers(self, srvr: str) -> list:
        try:
            # endpoint/srvrs/{id} -> get
            ep = f"{self.endpoint}srvrs/{srvr}"
            response = requests.get(ep)

            if response.status_code == 200:
                content = json.loads(response.content)

                is_ok = content.get("ok", False)
                error_type = content.get("errorType")

                if error_type:
                    raise EndpointException(
                        ep,
                        content["errorMessage"],
                        f"**Mr.Lobot** cannot obtain the guild for this server")
                elif not is_ok:
                    raise EndpointException(
                        ep,
                        content["message"],
                        f"**Mr.Lobot** cannot obtain the guild for this server")
                else:
                    return {
                        "ok": 1,
                        "content": content["content"]["items"]
                    }
            else:
                raise Exception(f"**Mr.Lobot** found a problem while"
                                f"gathering the guild from the server **{srvr}**:"
                                f"Status **{response.status_code}**")
        except Exception as ex:
            logger.error(ex)
            raise ex

    def add_to_spreadsheet(self, sheet: str, guild: str, chars: List[int]):
        try:
            # endopoint/spreadsheet/sheet_name -> put
            ep = f"{self.endpoint}spreadsheet/{sheet}"
            payload = {
                "guild": str(guild),
                "chars": chars
            }

            response = requests.put(ep, data=json.dumps(payload))

            if response.status_code == 200:
                content = json.loads(response.content)
                is_ok = content.get("ok", False)

                error_type = content.get("errorType")

                if error_type:
                    raise EndpointException(
                        ep,
                        content["errorMessage"],
                        f"**Mr.Lobot** cannot add units to the spreadsheet **{sheet}**",
                        payload)
                elif not is_ok:
                    raise EndpointException(
                        ep,
                        content["message"],
                        f"**Mr.Lobot** cannot add units to the spreadsheet **{sheet}**",
                        payload)
                else:
                    content = content["content"]
                    return {
                        "ok": 1,
                        "added": content["new"],
                        "repeated": content["old"],
                        "is_new": content["is_new"]
                    }
            else:
                raise Exception(
                    f"**Mr.Lobot** found a problem while "
                    f"adding to the spreadsheet **{sheet}**: "
                    f"Status **{response.status_code}**")
        except Exception as ex:
            logger.error(ex)
            raise ex

    def remove_to_spreadsheet(self, sheet: str, guild: str, chars: List[int] = []):
        try:
            # endopoint/spreadsheet/sheet_name -> delete
            ep = f"{self.endpoint}spreadsheet/{sheet}"

            payload = {
                "guild": str(guild),
            }

            if len(chars) != 0:
                payload["chars"] = chars

            response = requests.delete(ep, data=json.dumps(payload))

            if response.status_code == 200:
                content = json.loads(response.content)

                is_ok = content.get("ok", False)
                error_type = content.get("errorType")

                if error_type:
                    raise EndpointException(
                        ep,
                        content["errorMessage"],
                        f"**Mr.Lobot** cannot remove units to the spreadsheet **{sheet}**",
                        payload)
                elif not is_ok:
                    if len(content["content"]) == 0:
                        raise EndpointException(
                            ep,
                            content["message"],
                            f"**Mr.Lobot** did not find the spreadsheet **{sheet}**",
                            payload)
                    else:
                        raise EndpointException(
                            ep,
                            content["message"],
                            f"**Mr.Lobot** cannot remove units to the spreadsheet **{sheet}**",
                            payload)
                else:
                    return {
                        "message": content["message"],
                        "deleted": content["content"]["deleted"],
                        "left": content["content"]["left"],
                        "sheet_removed": content["content"]["sheet_removed"]
                    }

            else:
                raise Exception(f"**Mr.Lobot** found a problem while removing"
                                f" to the spreadsheet **{sheet}**: "
                                f"Status **{response.status_code}**")
        except Exception as ex:
            logger.error(ex)
            raise ex

    def get_spreadsheet(self, guild: str, sheet: str):
        try:
            # endopoint/spreadsheet/guild -> get
            ep = f"{self.endpoint}spreadsheet/{sheet}/{guild}"
            response = requests.get(ep)

            if response.status_code == 200:
                content = json.loads(response.content)

                is_ok = content.get("ok", False)
                error_type = content.get("errorType")

                if error_type:
                    raise EndpointException(
                        ep,
                        content["errorMessage"],
                        f"**Mr.Lobot** cannot retrieve info for the spreadsheet **{sheet}**")
                elif not is_ok:
                    raise EndpointException(
                        ep,
                        content["message"],
                        f"**Mr.Lobot** cannot retrieve info for the spreadsheet **{sheet}**")
                else:
                    content = content["content"]

                    return {
                        "ok": 1,
                        "sheet": content["sheet"]
                    }
            else:
                raise Exception(f"**Mr.Lobot** found a problem while "
                                f"searching the guild **{guild}** spreadsheets: "
                                f"Status **{response.status_code}**")
        except Exception as ex:
            logger.error(ex)
            raise ex

    def guild_spreadsheets(self, guild: str, start_expression: str):
        try:
            # endopoint/spreadsheets/guild?options -> get
            ep = f"{self.endpoint}spreadsheets/{guild}?start={start_expression}"
            response = requests.get(ep)

            if response.status_code == 200:
                content = json.loads(response.content)

                is_ok = content.get("ok", False)
                error_type = content.get("errorType")

                if error_type:
                    raise EndpointException(
                        ep,
                        content["errorMessage"],
                        f"**Mr.Lobot** cannot obtain spreadsheets for the guild **{guild}**")
                elif not is_ok:
                    raise EndpointException(
                        ep,
                        content["message"],
                        f"**Mr.Lobot** cannot obtain spreadsheets for the guild **{guild}**")
                else:
                    content = content["content"]

                    return {
                        "ok": 1,
                        "sheets": content["sheets"]
                    }
            else:
                raise Exception(f"**Mr.Lobot** a problem while searching "
                                f"the guild **{guild}** spreadsheets: "
                                f"Status **{response.status_code}**")
        except Exception as ex:
            logger.error(ex)
            raise ex
