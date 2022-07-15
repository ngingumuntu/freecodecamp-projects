import numpy as np
import pandas as pd


def get_channels_stats(youtube, channel_ids) -> pd.DataFrame:
    
    all_data = []
    
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=','.join(channel_ids)
    )
    response = request.execute()
    
    for i in range(len(response['items'])):
        data = dict(
            title       = response['items'][i]['snippet']['title'],
            views       = response['items'][i]['statistics']['viewCount'],
            subscribers = response['items'][i]['statistics']['subscriberCount'],
            totalVideos = response['items'][i]['statistics']['videoCount'],
            playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
        )
        all_data.append(data)
        
    return pd.DataFrame(all_data) 

def get_video_ids(youtube, playlist_ids) -> list:
    request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_ids,
                maxResults = 50)
    response = request.execute()
    
    video_ids = []
    
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
        
    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId = playlist_ids,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()
    
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            
            next_page_token = response.get('nextPageToken')
        
    return video_ids

def get_video_details(youtube, video_ids) -> pd.DataFrame:
    all_videos = []
    
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute() 

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition', 'caption']
                            }
            video_stats = {}
            video_stats['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_stats[v] = video[k][v]
                    except:
                        video_stats[v] = None

            all_videos.append(video_stats)
        
    return pd.DataFrame(all_videos)

def get_comments_in_videos(youtube, video_ids) -> pd.DataFrame:
    all_comments = []
    
    for video_id in video_ids:
        try:   
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id
            )
            response = request.execute()
        
            comments_in_video = [comment['snippet']['topLevelComment']['snippet']['textOriginal'] for comment in response['items'][0:10]]
            comments_in_video_info = {'video_id': video_id, 'comments': comments_in_video}

            all_comments.append(comments_in_video_info)
            
        except:
            print('Could not get comments for video ' + video_id)
        
    return pd.DataFrame(all_comments) 
