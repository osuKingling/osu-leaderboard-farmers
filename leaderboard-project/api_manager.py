import os
from ossapi import Ossapi, OssapiV1, OssapiAsync


class APIManager:

    def __init__(self):

        self.apiv1 = OssapiV1(os.environ['API_KEY_V1'])
        self.apiv2 = Ossapi(int(os.environ['API_ID']), os.environ['API_SECRET'])
        self.apiv2async = OssapiAsync(int(os.environ['API_ID']), os.environ['API_SECRET'])

        self.apiv2.beatmap_attributes(53)

    def retrieve_beatmaps(self, since):
        try:
            beatmaps = self.apiv1.get_beatmaps(since=since)
            return beatmaps
        except Exception as e:
            print(e)

    def retrieve_beatmap_scores(self, beatmap_ids):
        scores = []
        try:
            params = {"mode": "osu", "limit": 100}
            for id in beatmap_ids:
                # print(id, dt.now().strftime("%H:%M:%S:%f"))
                scores.append(
                    self.apiv2.session.get(f'https://osu.ppy.sh/api/v2/beatmaps/{id}/scores', params=params).json()[
                        'scores'])
            return scores
        except Exception as e:
            print(e)

    def retrieve_user_data(self, username: str):
        return self.apiv2.user(username)


api_mgr = APIManager()
