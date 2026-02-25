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
    async def fetch_comments(post_ids: List[str], platform: str, fb_token: str = None, fallback_token: str = None, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
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
                            "fields": "id,username,text,timestamp,like_count",
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
                                    "like_count": comment.get("like_count", 0),
                                    "timestamp": comment.get("timestamp")
                                })
                        else:
                            raise Exception(f"Instagram API Error: {data.get('error', {}).get('message', 'Unknown Error')}")
                    else:
                        # 1. Fetch Facebook Comments
                        resp = await client.get(f"https://graph.facebook.com/v19.0/{post_id}/comments", params={
                            "access_token": fb_token,
                            "fields": "id,from,message,created_time,attachment,like_count",
                            "filter": "stream" if filters.get("include_replies") else "toplevel",
                            "summary": "1",
                            "limit": 1000
                        })
                        data = resp.json()
                        
                        # Fallback Strategy: If Page Token fails with OAuthException (like #10 pages_read_engagement), retry with User Token
                        if "error" in data and fallback_token and fallback_token != fb_token:
                            print(f"[Giveaway Debug] Page Token failed for post {post_id}: {data['error'].get('message')}. Retrying with User Fallback Token...")
                            retry_resp = await client.get(f"https://graph.facebook.com/v19.0/{post_id}/comments", params={
                                "access_token": fallback_token,
                                "fields": "id,from,message,created_time,attachment,like_count",
                                "filter": "stream" if filters.get("include_replies") else "toplevel",
                                "summary": "1",
                                "limit": 1000
                            })
                            retry_data = retry_resp.json()
                            if "error" not in retry_data:
                                print(f"[Giveaway Debug] Fallback Token succeeded for post {post_id}.")
                                data = retry_data
                                # Switch the token for subsequent requests
                                fb_token = fallback_token
                        
                        # Fallback Strategy 2: If both fail, the Graph API might be rejecting the compound `pageid_postid` format for the comments edge.
                        # Try stripping the `pageid_` prefix if it exists.
                        if "error" in data and "_" in post_id:
                            base_post_id = post_id.split("_")[1]
                            print(f"[Giveaway Debug] Both tokens failed. Stripping prefix and retrying with base ID: {base_post_id}...")
                            
                            # Let's try the page token first on the base ID
                            retry_resp_base = await client.get(f"https://graph.facebook.com/v19.0/{base_post_id}/comments", params={
                                "access_token": fb_token,
                                "fields": "id,from,message,created_time,attachment,like_count",
                                "filter": "stream" if filters.get("include_replies") else "toplevel",
                                "summary": "1",
                                "limit": 1000
                            })
                            
                            retry_base_data = retry_resp_base.json()
                            if "error" not in retry_base_data:
                                print(f"[Giveaway Debug] Base ID {base_post_id} succeeded with fb_token.")
                                data = retry_base_data
                                post_id = base_post_id # Update post_id for the likes API
                            elif fallback_token:
                                # Try user token on base ID
                                retry_resp_base_user = await client.get(f"https://graph.facebook.com/v19.0/{base_post_id}/comments", params={
                                    "access_token": fallback_token,
                                    "fields": "id,from,message,created_time,attachment,like_count",
                                    "filter": "stream" if filters.get("include_replies") else "toplevel",
                                    "summary": "1",
                                    "limit": 1000
                                })
                                retry_base_user_data = retry_resp_base_user.json()
                                if "error" not in retry_base_user_data:
                                    print(f"[Giveaway Debug] Base ID {base_post_id} succeeded with user fallback_token.")
                                    data = retry_base_user_data
                                    post_id = base_post_id
                                    fb_token = fallback_token
                                
                        if "error" in data:
                            raise Exception(f"Facebook API Error: {data['error'].get('message', 'Unknown Error')}")
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
                                    "like_count": comment.get("like_count", 0),
                                    "timestamp": comment.get("created_time")
                                })
                        else:
                            raise Exception(f"Facebook API Error: {data.get('error', {}).get('message', str(data))}")
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
        """Filters the comment list based on advanced giveaway rules."""
        filtered = comments

        # 1. Exclude Users
        excluded_users_str = filters.get("excluded_users", "")
        if excluded_users_str:
            excluded = [u.strip().lower() for u in excluded_users_str.split(",") if u.strip()]
            if excluded:
                filtered = [c for c in filtered if c.get("user_name", "").lower() not in excluded and c.get("user_id", "").lower() not in excluded]

        # 2. Minimum Comment Likes
        min_likes = filters.get("min_comment_likes", 0)
        if min_likes > 0:
            filtered = [c for c in filtered if c.get("like_count", 0) >= min_likes]

        # 3. Filter Mentions (Require minimum number of @mentions)
        min_mentions = filters.get("min_mentions", 0)
        if min_mentions > 0:
            mention_filtered = []
            for c in filtered:
                mentions_count = len(re.findall(r'@\w+', c["text"]))
                if mentions_count >= min_mentions:
                    mention_filtered.append(c)
            filtered = mention_filtered
            
        # 4. Filter by Specific Word / Hashtag
        required_word = filters.get("required_word", "").strip().lower()
        if required_word:
            word_filtered = []
            conditions = [req.strip() for req in required_word.split(",")] if "," in required_word else [required_word]
            for c in filtered:
                comment_text = c["text"].lower()
                if any(cond in comment_text for cond in conditions):
                    word_filtered.append(c)
            filtered = word_filtered

        # 5. Filter by Photo requirement
        if filters.get("require_photo"):
            filtered = [c for c in filtered if c.get("has_photo")]

        # 6. Filter by Date Range
        start_date_str = filters.get("start_date")
        end_date_str = filters.get("end_date")
        if start_date_str or end_date_str:
            start_dt = datetime.strptime(start_date_str, "%Y-%m-%d").date() if start_date_str else None
            end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").date() if end_date_str else None
            
            date_filtered = []
            for c in filtered:
                c_time_str = c.get("timestamp")
                if c_time_str:
                    try:
                        c_date = datetime.strptime(c_time_str[:10], "%Y-%m-%d").date()
                        if start_dt and c_date < start_dt:
                            continue
                        if end_dt and c_date > end_dt:
                            continue
                        date_filtered.append(c)
                    except Exception:
                        pass
            filtered = date_filtered

        # 7. Filter by Required Like
        if filters.get("require_like"):
            filtered = [c for c in filtered if c.get("liked_post", False)]

        # 8. Enforce Entry Limits & Apply Extra Entries
        if filters.get("filter_duplicates"):
            max_entries = 1
        else:
            max_entries_raw = filters.get("max_entries_per_user")
            try:
                max_entries = int(max_entries_raw) if max_entries_raw else float('inf')
                if max_entries <= 0: max_entries = float('inf')
            except (ValueError, TypeError):
                max_entries = float('inf')

        user_counts = {}
        limited_comments = []
        for c in filtered:
            uid = c["user_id"]
            user_counts[uid] = user_counts.get(uid, 0) + 1
            if user_counts[uid] <= max_entries:
                limited_comments.append(c)
                
        filtered = limited_comments

        # 9. Extra Entries
        extra_entries_str = filters.get("extra_entries", "")
        if extra_entries_str:
            vip_users = [u.strip().lower() for u in extra_entries_str.split(",") if u.strip()]
            if vip_users:
                from collections import Counter
                vip_counts = Counter(vip_users)
                
                extra_comments = []
                seen_vip_uids = set()
                
                for c in filtered:
                    uname = c.get("user_name", "").lower()
                    uid = c.get("user_id", "").lower()
                    
                    if uid not in seen_vip_uids:
                        bonus = vip_counts.get(uname, 0) or vip_counts.get(uid, 0)
                        if bonus > 0:
                            extra_comments.extend([c] * bonus)
                            seen_vip_uids.add(uid)
                            
                filtered.extend(extra_comments)

        random.shuffle(filtered)
        return filtered

    @staticmethod
    async def draw_winners(db: AsyncSession, post_ids: List[str], platform: str, num_winners: int, filters: Dict[str, Any], fb_token: str = None, fallback_token: str = None, preview_only: bool = False) -> List[Dict[str, Any]]:
        """
        Executes the full Giveaway pipeline: Fetch -> Filter -> Draw.
        If preview_only is True, returns ALL eligible comments without drawing.
        """
        raw_comments = await GiveawayService.fetch_comments(post_ids, platform, fb_token, fallback_token=fallback_token, filters=filters)
        
        eligible_comments = GiveawayService.apply_filters(raw_comments, filters)
        
        if not eligible_comments:
            raise Exception(f"0 participants! Total fetched from Facebook: {len(raw_comments)}. All were dropped by your filters.")

        if preview_only:
            return eligible_comments

        # Ensure we don't try to pick more winners than eligible pools
        actual_winners_count = min(num_winners, len(eligible_comments))
        
        # Random choice without replacement
        winners = random.sample(eligible_comments, actual_winners_count)
        
        # We could save this to the DB if we wanted to log the campaign right away
        # For now, we return it to the frontend for the animation!
        
        return winners
