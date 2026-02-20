import random
import os
import re
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import GiveawayCampaign, GiveawayWinner

class GiveawayService:
    @staticmethod
    async def fetch_comments(post_id: str, platform: str) -> List[Dict[str, Any]]:
        """
        Fetches comments from the Meta Graph API if keys exist.
        Otherwise, returns a robust set of simulated comments for the Demo.
        """
        fb_token = os.getenv("FB_ACCESS_TOKEN")
        
        if fb_token and "demo" not in post_id:
            # TODO: Implement actual `httpx` call to Meta Graph API here
            # e.g. GET https://graph.facebook.com/v19.0/{post_id}/comments
            return []
        
        # --- DEMO MODE ---
        return GiveawayService._generate_demo_comments()


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

        return filtered

    @staticmethod
    async def draw_winners(db: AsyncSession, post_id: str, platform: str, num_winners: int, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Executes the full Giveaway pipeline: Fetch -> Filter -> Draw.
        """
        raw_comments = await GiveawayService.fetch_comments(post_id, platform)
        
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
