from discord.ext.commands import Cog
from discord import Embed
from discord.ext.commands import command
from discord.utils import get
from discord.ext.menus import MenuPages, ListPageSource

from utils.logger import get_logger

logger = get_logger(__name__)


class Help(ListPageSource):
    def __init__(self, ctx, data):
        self.ctx = ctx
        super().__init__(data, per_page=3)

    async def write_page(self, menu, fields):
        offset = (menu.current_page*self.per_page) + 1
        len_data = len(self.entries)

        embed = Embed(title="MrLobot Help")
        embed.set_footer(text=f"For more info visit [https://thecopy-and-thepaste.github.io/mrlobot.html]")
        for name, value in fields:
            embed.add_field(name=name, value=value, inline=False)

        return embed

    async def format_page(self, menu, entries):
        fields = []

        from pdb import set_trace as bp

        for entry in entries:
            if entry.name == "help":
                continue

            fields.append(
                ("command", f'**+{entry.name}** [{" ".join(["+"+a for a in entry.aliases])}]'))
            fields.append(("usage", entry.help or "Not Available"))
            fields.append(
                ("description", entry.description or "Not Available"))

        return await self.write_page(menu, fields)


class BotInfo(Cog):
    def __init__(self, bot) -> None:
        self.bot = bot

    @command(name="version",
             aliases=["v"],
             description=(f"Shows the current version of the bot"))
    async def show_version(self, ctx):
        await ctx.send(f"**Mr.Lobot** is running @{self.bot.version} version.")

    @command(name="help")
    async def show_help(self, ctx):
        menu = MenuPages(source=Help(ctx, list(self.bot.commands)))
        await menu.start(ctx)


def setup(bot):
    logger.info(f"Adding {BotInfo.__name__}")
    bot.add_cog(BotInfo(bot))
