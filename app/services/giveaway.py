import random
import os
import re
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import GiveawayCampaign, GiveawayWinner

import httpx

class GiveawayService:
    @staticmethod
    async def fetch_comments(post_ids: List[str], platform: str, fb_token: str = None, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        if filters is None: filters = {}
        all_comments = []
        is_demo = any("demo" in p or "post_" in p for p in post_ids)
        
        if fb_token and not is_demo:
            async with httpx.AsyncClient() as client:
                for post_id in post_ids:
                    if platform == "instagram":
                        # 1. Fetch Instagram Comments
                        resp = await client.get(f"https://graph.facebook.com/v19.0/{post_id}/comments", params={
                            "access_token": fb_token,
                            "fields": "id,username,text,timestamp",
                            "limit": 1000
                        })
                        data = resp.json()
                        
                        if "data" in data:
                            for comment in data["data"]:
                                # Instagram returns username directly in the root of the comment object
                                user_name = comment.get("username", "Anonymous Instagram User")
                                all_comments.append({
                                    "id": comment.get("id"),
                                    "user_id": user_name, # Username is unique on IG, acting as the ID for duplication logic
                                    "user_name": user_name,
                                    "profile_pic_url": f"https://ui-avatars.com/api/?name={user_name.split(' ')[0]}&background=random&color=fff",
                                    "text": comment.get("text", ""),
                                    "has_photo": False,
                                    "liked_post": True,
                                    "timestamp": comment.get("timestamp")
                                })
                    else:
                        # 1. Fetch Facebook Comments
                        resp = await client.get(f"https://graph.facebook.com/v19.0/{post_id}/comments", params={
                            "access_token": fb_token,
                            "fields": "id,from,message,created_time,attachment",
                            "filter": "stream" if filters.get("include_replies") else "toplevel",
                            "summary": "1",
                            "limit": 1000
                        })
                        data = resp.json()
                        
                        # 2. Fetch Facebook Likes if requested (Premium Filter)
                        post_likers = set()
                        if filters.get("require_like"):
                            likes_resp = await client.get(f"https://graph.facebook.com/v19.0/{post_id}/likes", params={
                                "access_token": fb_token,
                                "limit": 1000
                            })
                            if likes_resp.status_code == 200:
                                likes_data = likes_resp.json()
                                if "data" in likes_data:
                                    post_likers = {like["id"] for like in likes_data["data"]}
                        
                        if "data" in data:
                            for comment in data["data"]:
                                from_user = comment.get("from", {})
                                user_id = from_user.get("id", "unknown_id")
                                user_name = from_user.get("name", "Unknown User")
                                all_comments.append({
                                    "id": comment.get("id"),
                                    "user_id": user_id,
                                    "user_name": user_name,
                                    "profile_pic_url": f"https://ui-avatars.com/api/?name={user_name.split(' ')[0]}&background=random&color=fff",
                                    "text": comment.get("message", ""),
                                    "has_photo": "attachment" in comment and comment["attachment"].get("type") == "photo",
                                    "liked_post": user_id in post_likers if filters.get("require_like") else True,
                                    "timestamp": comment.get("created_time")
                                })
            return all_comments
        
        # --- DEMO MODE ---
        # If they selected multiple demo posts, we just generate a massive pool
        for _ in post_ids:
            all_comments.extend(GiveawayService._generate_demo_comments())
            
        return all_comments


    @staticmethod
    def _generate_demo_comments() -> List[Dict[str, Any]]:
        """Generates realistic-looking test comments for the animation."""
        names = ["Aymen", "Sarra", "Walid", "Rim", "Mehdi", "Fatma", "Khaled", "Amira", "Youssef", "Ines", 
                 "Zied", "Safa", "Marwen", "Nour", "Seif", "Hiba", "Oussama", "Yasmine", "Ali", "Zeineb"]
        
        base_comments = [
            "Participate! @friend1 @friend2",
            "I love this Bijoux! 😍",
            "Moi svp!",
            "Top! @someone",
            "J'espère gagner! 🙏",
            "Magnifique collection",
            "Tagging my bestie @bff",
            "C'est pour moi ça!",
            "Très beau",
            "InchaAllah je gagne @brother @sister"
        ]
        
        comments = []
        # Generate 50 comments
        for i in range(50):
            # 20% chance of being a duplicate user
            user_id = f"user_{random.randint(1, 40)}"
            name = random.choice(names) + " " + str(random.randint(10, 99))
            text = random.choice(base_comments)
            
            # 10% chance to add extra tags to test the mentions filter
            if random.random() > 0.9:
                text += " @random_friend"
                
            comments.append({
                "id": f"comment_{i}",
                "user_id": user_id,
                "user_name": name,
                "profile_pic_url": f"https://ui-avatars.com/api/?name={name.split(' ')[0]}&background=random&color=fff",
                "text": text,
                "timestamp": datetime.utcnow().isoformat()
            })
            
        return comments

    @staticmethod
    def apply_filters(comments: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Filters the comment list based on giveaway rules."""
        filtered = comments

        # 1. Filter Duplicates (Keep only 1 entry per user based on user_id)
        if filters.get("filter_duplicates"):
            seen_users = set()
            unique_comments = []
            for c in filtered:
                if c["user_id"] not in seen_users:
                    unique_comments.append(c)
                    seen_users.add(c["user_id"])
            filtered = unique_comments

        # 2. Filter Mentions (Require minimum number of @mentions)
        min_mentions = filters.get("min_mentions", 0)
        if min_mentions > 0:
            mention_filtered = []
            for c in filtered:
                # Count instances of '@' followed by word characters
                mentions_count = len(re.findall(r'@\w+', c["text"]))
                if mentions_count >= min_mentions:
                    mention_filtered.append(c)
            filtered = mention_filtered
            
        # 3. Filter by Specific Word / Hashtag
        required_word = filters.get("required_word", "").strip()
        if required_word:
            word_filtered = []
            for c in filtered:
                # Case insensitive search
                if required_word.lower() in c["text"].lower():
                    word_filtered.append(c)
            filtered = word_filtered

        # 4. Filter by Photo requirement
        if filters.get("require_photo"):
            filtered = [c for c in filtered if c.get("has_photo")]

        # 5. Filter by Date Limit
        date_limit_str = filters.get("date_limit")
        if date_limit_str:
            try:
                limit_dt = datetime.strptime(date_limit_str, "%Y-%m-%d").date()
                date_filtered = []
                for c in filtered:
                    c_time_str = c.get("timestamp")
                    if c_time_str:
                        c_date = datetime.strptime(c_time_str[:10], "%Y-%m-%d").date()
                        if c_date >= limit_dt:
                            date_filtered.append(c)
                filtered = date_filtered
            except Exception:
                pass

        # 6. Filter by Required Like
        if filters.get("require_like"):
            # Check the boolean flag we set during fetch
            filtered = [c for c in filtered if c.get("liked_post", False)]

        return filtered

    @staticmethod
    async def draw_winners(db: AsyncSession, post_ids: List[str], platform: str, num_winners: int, filters: Dict[str, Any], fb_token: str = None) -> List[Dict[str, Any]]:
        """
        Executes the full Giveaway pipeline: Fetch -> Filter -> Draw.
        """
        raw_comments = await GiveawayService.fetch_comments(post_ids, platform, fb_token, filters)
        
        eligible_comments = GiveawayService.apply_filters(raw_comments, filters)
        
        if not eligible_comments:
            return [] # No one eligible

        # Ensure we don't try to pick more winners than eligible pools
        actual_winners_count = min(num_winners, len(eligible_comments))
        
        # Random choice without replacement
        winners = random.sample(eligible_comments, actual_winners_count)
        
        # We could save this to the DB if we wanted to log the campaign right away
        # For now, we return it to the frontend for the animation!
        
        return winners
