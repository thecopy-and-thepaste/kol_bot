import pydash
import os

import pandas as pd

from logging import exception
from discord import Embed, File
from discord.ext import commands
from discord.ext.commands import Cog
from discord.ext.commands import command
from discord.ext.commands.errors import CommandInvokeError

from providers.swgoh import SWGOH
from providers.endpoints import MrLobotStorageEndpoint

# from operator import itemgetter
# from nltk import ngrams
from typing import List, Optional

from utils.logger import get_logger
from utils.exc import EndpointException

from pdb import set_trace as bp

logger = get_logger(__name__)


class MrSpreadsheet(Cog):
    def __init__(self, bot) -> None:
        self.bot = bot
        # self.db = MrLobotDB()
        # self.chars = pd.DataFrame()
        self.alias_char = {}
        self.srvr_guilds = {}

        self.swgoh = SWGOH()
        self.mrlobot_storage = MrLobotStorageEndpoint()

    @Cog.listener()
    async def on_ready(self):
        self.chars, self.alias_char = self.swgoh.get_chars()
        logger.info("Chars built")

        # action = ["abilities", "list"]
        # result = self.client.action(self.schema, action)

        print("MrLobot spreadsheets ready")

    # region auxiliar methods
    def __check_guild(self, _, id_guild):
        try:
            res = self.swgoh.guild_info(id_guild)
            return {"ok": 1,
                    "response": res,
                    "message": res["name"]}
        except EndpointException as ex:
            raise ex
        except Exception as ex:
            logger.error(ex)
            raise ex

    def __check_chars(self, chars: List[int]):
        try:
            correct = {}
            unk = set()

            for char in chars:
                try:
                    char_info = self.alias_char[char.strip()]
                    correct[char_info["id"]] = {
                        "id": char_info["id"],
                        "name": char_info["name"]
                    }
                except:
                    unk.add(char)

            return correct, unk
        except Exception as ex:
            logger.error(ex)
            raise

    def get_guild(self, srvr: str):
        try:
            guild = self.srvr_guilds.get(str(srvr))

            if not guild:
                logger.warn(f"This stuff is expensive: scanning for {srvr}")

                try:
                    response = self.mrlobot_storage\
                        .guild_servers(srvr)
                except Exception as ex:
                    # Endpoint error, we don't need custom messaga
                    raise ex

                temp = response["content"]

                if len(temp) == 0 or temp is None:
                    raise Exception((f"**Mr.Lobot** did not find any guild in this server.\n"
                                     f"Use **+mrlobot_config guild \\guild_id\\ ** to add it."))
                else:
                    self.srvr_guilds\
                        .update({item["srvr"]: item["guild"] for item in temp})

                # self.srvr_guilds = {'628798768355082260': '69571'}

            guild = self.srvr_guilds.get(str(srvr))

            return guild
        except Exception as ex:
            logger.error(ex)
            raise ex

    def make_spreadsheets(self, guild: str, char_ids: List[int], stat_opts: List[str]) -> List[pd.DataFrame]:
        def relic(unit_data: dict) -> int:
            r_level = int(unit_data["relic_tier"]) - 2

            return r_level*(r_level > 0)

        def def_processor(sheet: pd.DataFrame) -> pd.DataFrame:
            sheet = sheet.fillna(0)
            data_cols = sheet.columns[1:]
            temp = sheet[data_cols].astype("int32")
            sheet[data_cols] = temp

            return sheet

        REPORT_MAPPER = {
            "pg": lambda x: x["power"] or 0,
            "relic": relic,
        }

        SHEET_PROCESSOR = {
            "pg": def_processor,
            "relic": def_processor,
        }

        players_stats = self.swgoh.get_guild_players(guild)
        base_ids = self.chars[self.chars["id"]
                              .isin(char_ids)]["base_id"].unique().tolist()

        mrlobot_sheets = []
        unit_names = {x["base_id"]: x["name"]
                      for x in self.alias_char.values()}
        unit_names["PLAYER"] = "PLAYER"

        for stat in stat_opts:
            player_stats = []

            for pstats in players_stats:

                name = pstats["data"].get("name")
                unit_stats = pydash.chain(pstats["units"])\
                    .map(lambda x: x["data"])\
                    .filter(lambda x: x["base_id"] in base_ids)\
                    .map(lambda x: (x["base_id"], REPORT_MAPPER.get(stat, "")(x)))\
                    .value()

                unit_stats = dict(unit_stats)
                unit_stats["PLAYER"] = name

                player_stats.append(unit_stats)

            temp = pd.DataFrame(player_stats, columns=["PLAYER", *base_ids])
            temp.columns = temp.columns.to_series().map(unit_names)
            mrlobot_sheets.append(
                (stat, SHEET_PROCESSOR[stat](temp))
            )

        return mrlobot_sheets

    # endregion

    # region commands

    CONFIG_OPTIONS_MAPPER = {
        "guild": lambda self, ctx, x: self.mrlobot_storage.register_guild(*x)
    }

    CONFIG_PRES = {
        "guild": lambda self, x: self.__check_guild(*x)
    }

    @command(name="mrlobot_config", aliases=["config", "c"])
    async def mrlobot_config(self, ctx, option: str, *, value: str):
        def exc_wrapper(param):
            raise Exception(f"{param} is not a valid config option")

        try:
            sent = (ctx.guild.id, value)

            requirements = self.CONFIG_PRES\
                .get(option,
                     lambda _, x: exc_wrapper(option)
                     )(self, sent)

            if requirements["ok"]:
                response = self.CONFIG_OPTIONS_MAPPER\
                    .get(option,
                         lambda _, x: exc_wrapper(option)
                         )(self, ctx, sent)

                await ctx.send(f"**Mr.Lobot** has successfully stored the config option: {option} : {requirements['message']}")
            else:
                raise Exception(requirements["message"])
        except Exception as ex:
            logger.error(ex)
            raise(ex)

    @mrlobot_config.error
    async def on_mrlobot_config_error(self, ctx, ex):
        try:
            if not isinstance(ex, commands.MissingRequiredArgument):
                if hasattr(ex, "original"):
                    await ctx.send(ex.original)
                else:
                    logger.erro("Unknown exception @on_mrlobot_config_error")
                    logger.error(ex)

        except Exception as ex:
            raise ex

    @command(name="mrlobot_listchars", aliases=["list", "l"])
    async def mrlobot_listchars(self, ctx, start: Optional[str]):
        try:
            chars_to_list = self.chars
            if start:
                ixs = self.chars.apply(
                    lambda x: x["name"].startswith(start), axis=1)
                chars_to_list = self.chars[ixs]

            grouped_chars = chars_to_list.groupby("name").indices

            if len(grouped_chars) > 0:
                await ctx.send("This is gonna take looong")

                embed = Embed(title=f"Chars")
                need_send = False

                for ix, (name, row_ixs) in enumerate(grouped_chars.items()):
                    ix += 1
                    aliases = chars_to_list.iloc[row_ixs]\
                        .apply(lambda x: x["alias"], axis=1).tolist()

                    embed.add_field(name="name",
                                    value=name,
                                    inline=False)
                    embed.add_field(name="alias",
                                    value="\n".join([f"> {x}" for x in aliases]))
                    need_send = True

                    if ix % 10 == 0:
                        await ctx.send(embed=embed)
                        embed = Embed(title=f"Chars")
                        need_send = False

                if need_send:
                    await ctx.send(embed=embed)
            else:
                await ctx.send((f"**Mr.Lobot** did not found any units starting with **{start}**"))

        except Exception as ex:
            logger.error(ex)
            raise

    @mrlobot_listchars.error
    async def on_mrlobot_listchars_error(self, ctx, ex):
        try:
            if not isinstance(ex, commands.MissingRequiredArgument):
                if hasattr(ex, "original"):
                    await ctx.send(ex.original)
                else:
                    logger.erro(
                        "Unknown exception @on_mrlobot_listchars_error")
                    logger.error(ex)
        except Exception as ex:
            raise ex

    @command(name="mrlobot_addsheet", aliases=["add", "a"])
    async def mrlobot_addsheet(self, ctx, sheet_name: str, *, chars: str):
        try:
            srvr_id = str(ctx.guild.id)
            guild = self.get_guild(srvr_id)

            chars = chars.split(",")

            correct_chars, error_chars = self.__check_chars(chars)

            error_chars_embed = Embed(
                title=f"**Mr.Lobot** didn't find the following chars",
                color=0xff0000)

            if len(error_chars) > 0:
                error_chars_embed.add_field(name="PRY char name",
                                            value="\n".join(map(lambda x: f"> {x}", error_chars)))
                await ctx.send(embed=error_chars_embed)

            if len(correct_chars) > 0:
                try:
                    response = self.mrlobot_storage\
                        .add_to_spreadsheet(sheet=sheet_name,
                                            guild=guild,
                                            chars=[*correct_chars.keys()])
                except Exception as ex:
                    # Endpoint error, we don't need custom messaga
                    raise ex

                added, repeated, is_new = response["added"], response["repeated"], response["is_new"]

                title = f"New **Mr.Lobot Spreadsheet** {sheet_name} added with:" \
                    if is_new \
                    else f"**Mr.Lobot Spreadsheet** {sheet_name} modified units:"

                correct_chars_embed = Embed(
                    title=title,
                    color=0x00ff00)

                if len(added) > 0:
                    names = self.chars[self.chars["id"]
                                       .isin(added)]["name"].unique()
                    correct_chars_embed.add_field(name="Chars added",
                                                  value="\n".join(map(lambda x: f'> {x}', names)))

                if len(repeated) > 0:
                    names = self.chars[self.chars["id"]
                                       .isin(repeated)]["name"].unique()
                    correct_chars_embed.add_field(name="Chars already stored",
                                                  value="\n".join(map(lambda x: f'> {x}', names)))

                await ctx.send(embed=correct_chars_embed)
            else:
                await ctx.send("**Mr.Lobot** did not modify any spreadsheet")

        except Exception as ex:
            logger.error(ex)
            raise

    @mrlobot_addsheet.error
    async def on_mrlobot_addsheet_error(self, ctx, ex):
        try:
            if not isinstance(ex, commands.MissingRequiredArgument):
                if hasattr(ex, "original"):
                    await ctx.send(ex.original)
                else:
                    logger.erro("Unknown exception @on_mrlobot_addsheet_error")
                    logger.error(ex)
        except Exception as ex:
            raise ex

    @command(name="mrlobot_deletesheet", aliases=["delete", "d"])
    async def mrlobot_deletesheet(self, ctx, sheet_name: str, *, chars: Optional[str] = ""):
        try:
            srvr_id = str(ctx.guild.id)
            guild = self.get_guild(srvr_id)

            if len(chars) != 0:
                chars = chars.split(",")

            correct_chars, error_chars = self.__check_chars(chars)

            error_chars_embed = Embed(
                title=f"**Mr.Lobot** didn't find the following chars",
                color=0xff0000)
            correct_chars_embed = Embed(
                title=f"New **Mr.Lobot Spreadsheet** added with:",
                color=0x00ff00)

            if len(error_chars) > 0:
                error_chars_embed.add_field(name="PRY char name",
                                            value="\n".join(map(lambda x: f"> {x}", error_chars)))
                await ctx.send(embed=error_chars_embed)

            try:
                send_chars = [*correct_chars.keys()]
                # We send a dummy if chars are define
                # If we send empty array the complete table is deleted
                send_chars = send_chars + [-1] \
                    if len(chars) > 0 \
                    else send_chars

                response = self.mrlobot_storage\
                    .remove_to_spreadsheet(sheet=sheet_name,
                                           guild=guild,
                                           chars=send_chars)

                deleted = response["deleted"]
                left = response["left"]
                sheet_removed = response["sheet_removed"]
                msg = response["message"]
            except Exception as ex:
                # Endpoint error, we don't need custom messaga
                raise ex

            if sheet_removed:
                await ctx.send(f"**Mr.Lobot** succesfullty deleted the spreadsheet **{sheet_name}**")
            elif len(correct_chars) > 0:

                if len(deleted) > 0:
                    names = self.chars[self.chars["id"]
                                       .isin(deleted)]["name"].unique()
                    correct_chars_embed.add_field(name="Chars removed",
                                                  value="\n".join(map(lambda x: f'> {x}', names)))

                if len(left) > 0:
                    names = self.chars[self.chars["id"]
                                       .isin(left)]["name"].unique()
                    correct_chars_embed.add_field(name="Chars left in the spreadsheet",
                                                  value="\n".join(map(lambda x: f'> {x}', names)))

                await ctx.send(embed=correct_chars_embed)
            else:
                await ctx.send(f"**Mr.Lobot** did not modify the spreadsheet **{sheet_name}**")

        except Exception as ex:
            logger.error(ex)
            raise

    @mrlobot_deletesheet.error
    async def on_mrlobot_deletesheet_error(self, ctx, ex):
        try:
            if not isinstance(ex, commands.MissingRequiredArgument):
                if hasattr(ex, "original"):
                    await ctx.send(ex.original)
                else:
                    logger.erro(
                        "Unknown exception @on_mrlobot_deletesheet_error")
                    logger.error(ex)
        except Exception as ex:
            raise ex

    @command(name="mrlobot_showsheet", aliases=["show", "s"])
    async def mrlobot_showsheet(self, ctx, start: str):
        try:
            srvr_id = str(ctx.guild.id)
            guild = self.get_guild(srvr_id)

            try:
                if start == "*":
                    start = ""
                response = self.mrlobot_storage\
                    .guild_spreadsheets(guild=guild,
                                        start_expression=start)
            except Exception as ex:
                # Endpoint error, we don't need custom messaga
                raise ex

            spreadsheets = response["sheets"]
            if len(spreadsheets) == 0:
                await ctx.send((f"**Mr.Lobot** didn't find any spreadsheet for your guild starting with **{start}**\n"
                                f"Try another query or use the **+mrlobot_add** command to add a sheet"))
            else:
                await ctx.send((f"**Mr.Lobot** found the following stored sheet(s) for your"
                                f" guild starting with **{start}**"))

                for sheet in spreadsheets:
                    embed = Embed(
                        title=f"Sheet: {sheet['sheet']}")

                    chars = [int(ch) for ch in sheet["char_ids"]]
                    char_names = self.chars[self.chars["id"].isin(
                        chars)]["name"].unique()

                    embed.add_field(name="Chars",
                                    value="\n".join(map(lambda x: f'> {x}', char_names)))

                    await ctx.send(embed=embed)
        except Exception as ex:
            logger.error(ex)
            raise

    @mrlobot_showsheet.error
    async def on_mrlobot_showsheet_error(self, ctx, ex):
        try:
            if not isinstance(ex, commands.MissingRequiredArgument):
                if hasattr(ex, "original"):
                    await ctx.send(ex.original)
                else:
                    logger.erro(
                        "Unknown exception @on_mrlobot_showsheet_error")
                    logger.error(ex)
        except Exception as ex:
            raise ex

    VALID_SPREADSHEETS = {
        "pg": "Galactic Power",
        "relic": "Unit relic level"
    }

    @command(name="mrlobot_reportsheet", aliases=["report", "r"])
    async def mrlobot_reportsheet(self, ctx, sheet_name: str, *, to_report: str):
        try:
            opts = [*map(lambda x: x.strip(), to_report.split(","))]

            opts_to_report = {True: [],
                              False: []}

            for opt in opts:
                opts_to_report[opt in self.VALID_SPREADSHEETS].append(opt)

            if len(opts_to_report[False]) > 0:
                opts = " ".join(opts_to_report[False])
                embed = Embed(title=f"Not available options",
                              description=(f"**Mr. Lobot** cannot found the option(s): **{opts}**"
                                           f"\n Valid options are:"),
                              color=0xff0000)

                for item, val in self.VALID_SPREADSHEETS.items():
                    embed.add_field(name=f"{item}",
                                    value=f"**{val}**",
                                    inline=False)

                await ctx.send(embed=embed)

            if len(opts_to_report[True]) > 0:
                guild = self.get_guild(ctx.guild.id)
                try:
                    response = self.mrlobot_storage\
                        .get_spreadsheet(guild, sheet_name)
                except Exception as ex:
                    # Endpoint error, we don't need custom messaga
                    raise ex

                sheet = response["sheet"]

                if "char_ids" in sheet:
                    chars = list(map(lambda x: int(x), sheet["char_ids"]))

                    await ctx.send((f"Hold on, **Mr.Lobot** is making computations"))

                    mrlobot_sheets_info = self.make_spreadsheets(guild=guild,
                                                                 char_ids=chars,
                                                                 stat_opts=opts_to_report[True])

                    for attr, sheet in mrlobot_sheets_info:
                        filename = f"./{guild}_{sheet_name}_{to_report}.csv"

                        sheet.to_csv(filename, index=False)
                        await ctx.send(f"Spreadsheet {sheet_name} for {self.VALID_SPREADSHEETS[attr]}")
                        await ctx.send(file=File(filename))

                        os.remove(filename)

                else:
                    await ctx.send((f"**Mr.Lobot** did not locate the spreadsheet **{sheet_name}** "
                                    f"on his database."))
            else:
                await ctx.send((f"**Mr.Lobot** did not locate any stat to report"))
        except Exception as ex:
            raise ex

    @ mrlobot_reportsheet.error
    async def on_mrlobot_reportsheet_error(self, ctx, ex):
        try:
            if not isinstance(ex, commands.MissingRequiredArgument):
                if hasattr(ex, "original"):
                    await ctx.send(ex.original)
                else:
                    logger.erro(
                        "Unknown exception @on_mrlobot_reportsheet_error")
                    logger.error(ex)
        except Exception as ex:
            raise ex
    # endregion


def setup(bot):
    logger.info(f"Adding {MrSpreadsheet.__name__}")
    bot.add_cog(MrSpreadsheet(bot))
