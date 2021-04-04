import os

from discord.ext.commands import Bot as B
from discord.ext import commands
from discord.ext.commands import CommandNotFound

from pathlib import Path
from dotenv import load_dotenv

from utils.logger import get_logger

logger = get_logger(__name__)

PREFIX = "+"
COGS = [*Path("cogs").glob("*.py")]


class Bot(B):

    def __init__(self):
        load_dotenv()
        self.ready = False
        self.TOKEN = os.getenv('DISCORD_TOKEN')
        super().__init__(command_prefix=PREFIX)

    def setup(self):
        for cog in COGS:
            cog_module = cog.stem
            print(cog.name)
            self.load_extension(f"cogs.{cog_module}")

    def run(self, version):
        self.version = version
        self.setup()

        print("Bot running")
        return super().run(self.TOKEN, reconnect=True)

    async def on_connect(self):
        print("Bot connected")

    async def on_ready(self):
        self.ready = True
        print("Bot ready")

    async def on_error(self, err, *args):
        if isinstance(err, Exception):
            print("HERR EXCE")

        print(err)
        print("ARGS")
        print(args)

    async def on_command_error(self, ctx, ex):
        if isinstance(ex, commands.MissingRequiredArgument):
            await ctx.send(f"Refer to **+mrlobot_help** to pass all the required arguments "
                           f"of this command")
        else:
            logger.error("Command error")
            logger.error(ex)
