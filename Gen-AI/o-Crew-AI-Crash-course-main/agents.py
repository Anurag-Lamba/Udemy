from crewai import Agent
from tools import yt_tool

from dotenv import load_dotenv

load_dotenv()
from langchain_groq import ChatGroq
import httpx

import os
# os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
# os.environ["OPENAI_MODEL_NAME"]="gpt-4-0125-preview"

llm=ChatGroq(groq_api_key=os.getenv("sample_groq_api_key"),model_name="Gemma2-9b-It",http_client=httpx.Client(verify=False))


## Create a senior blog content researcher

blog_researcher=Agent(
    llm=llm,
    role='Blog Researcher from Youtube Videos',
    goal='get the relevant video transcription for the topic {topic} from the provided Yt channel',
    verboe=True,
    memory=True,
    backstory=(
       "Expert in understanding videos in AI Data Science , MAchine Learning And GEN AI and providing suggestion" 
    ),
    tools=[yt_tool],
    allow_delegation=True
)

## creating a senior blog writer agent with YT tool

blog_writer=Agent(
    llm=llm,
    role='Blog Writer',
    goal='Narrate compelling tech stories about the video {topic} from YT video',
    verbose=True,
    memory=True,
    backstory=(
        "With a flair for simplifying complex topics, you craft"
        "engaging narratives that captivate and educate, bringing new"
        "discoveries to light in an accessible manner."
    ),
    tools=[yt_tool],
    allow_delegation=False


)