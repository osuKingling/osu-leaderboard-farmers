import discord
import os
import bot_controller
from discord import app_commands
from table2ascii import table2ascii as t2a, PresetStyle

DISCORD_TOKEN = os.environ['DISCORD_TOKEN']

intents = discord.Intents.all()
intents.message_content = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


@tree.command(name="retrievebeatmapleaderboard")
async def retrieve_leaderboard(ctx, beatmap_id: int):
    try:
        leaderboard = bot_controller.retrieve_leaderboard(beatmap_id)
        beatmap_data = bot_controller.retrieve_beatmap_data(beatmap_id)
        embed = discord.Embed(
            title=f'{beatmap_data[4]} - {beatmap_data[3]} [{beatmap_data[6]}] mapped by {beatmap_data[21]}',
            description=f'```{leaderboard}```',
            color=0xFF5733)
        await ctx.response.send_message(embed=embed)
    except TypeError as e:
        print(e)
        await ctx.response.send_message("Beatmap ID Not Found")


@tree.command(name="top1sleaderboard")
async def top_1s_leaderboard(ctx, mods: str = None, max_acc: float = None,
                             min_acc: float = None, user_id: int = None,
                             max_length: int = None, min_length: int = None, min_stars: float = None,
                             max_stars: float = None, min_ar: float = None, max_ar: float = None, min_od: float = None,
                             max_od: float = None,
                             min_spinners: int = None, max_spinners: int = None, tag: str = None, page: int = 1,
                             combine_mods: bool = True):
    leaderboard_data, header_text = bot_controller.leaderboard(mods, max_acc, min_acc, user_id, max_length, min_length,
                                                               min_stars,
                                                               max_stars, min_ar, max_ar, min_od, max_od, min_spinners,
                                                               max_spinners,
                                                               tag, combine_mods)
    leaderboard_header = ['Rank', 'Username', 'Count']

    embeds = []

    if len(leaderboard_data) == 0:
        await ctx.response.send_message('No results for this query', ephemeral=True)
    else:

        for i in range(0, len(leaderboard_data), 10):
            embed = discord.Embed(
                title=f'#1 Leaderboard: {header_text}',
                description=f'```{t2a(header=leaderboard_header, body=leaderboard_data[i:i + 10], style=PresetStyle.borderless)}```',
                color=0xFF5733)
            embed.set_footer(text=f"Page {page}/{-(-len(leaderboard_data) // 10)}")
            embeds.append(embed)

        await ctx.response.send_message(embed=embeds[page - 1])


@tree.command(name="searchtop1s")
async def search_top_1s(ctx, mods: str = None, max_acc: float = None, min_acc: float = None, user_id: int = None,
                        max_length: int = None, min_length: int = None, min_stars: float = None,
                        max_stars: float = None, min_ar: float = None, max_ar: float = None, min_od: float = None,
                        max_od: float = None,
                        min_spinners: int = None, max_spinners: int = None, tag: str = None, combine_mods: bool = True):
    await ctx.response.send_message("CSV File:")
    buffer = bot_controller.retrieve_1s(mods, max_acc, min_acc, user_id, max_length, min_length, min_stars, max_stars,
                                        min_ar, max_ar, min_od, max_od,
                                        min_spinners, max_spinners, tag, combine_mods)
    await ctx.channel.send(file=discord.File(buffer, 'generated-csv.csv'))


@tree.command(name="link_account")
async def link_account(interaction: discord.Interaction, user_id: int):
    try:
        bot_controller.link_account(interaction.user.id, user_id)
        await interaction.response.send_message('Account linked successfully', ephemeral=True)
    except Exception as e:
        await interaction.response.send_message('Account linked failed', ephemeral=True)
        print(e)


@client.event
async def on_ready():
    await tree.sync()
    print("Ready")


if __name__ == "__main__":
    client.run(DISCORD_TOKEN)
