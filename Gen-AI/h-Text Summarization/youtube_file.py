from typing import Any, Dict, Generator, List, Optional, Sequence, Union
from urllib.parse import parse_qs, urlparse
from youtube_transcript_api import YouTubeTranscriptApi,FetchedTranscript
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
from langchain_core.documents import Document


def _parse_video_id(url: str) -> Optional[str]:
    """Parse a YouTube URL and return the video ID if valid, otherwise None."""




    ALLOWED_SCHEMES = {"http", "https"}
    ALLOWED_NETLOCS = {
        "youtu.be",
        "m.youtube.com",
        "youtube.com",
        "www.youtube.com",
        "www.youtube-nocookie.com",
        "vid.plus",
    }

    parsed_url = urlparse(url)

    if parsed_url.scheme not in ALLOWED_SCHEMES:
        return None

    if parsed_url.netloc not in ALLOWED_NETLOCS:
        return None

    path = parsed_url.path

    if path.endswith("/watch"):
        query = parsed_url.query
        parsed_query = parse_qs(query)
        if "v" in parsed_query:
            ids = parsed_query["v"]
            video_id = ids if isinstance(ids, str) else ids[0]
        else:
            return None
    else:
        path = parsed_url.path.lstrip("/")
        video_id = path.split("/")[-1]

    if len(video_id) != 11:  # Video IDs are 11 characters long
        return None

    return video_id
    


def get_best_transcript(video_url):
    video_id = _parse_video_id(video_url)
    ytt_api = YouTubeTranscriptApi()

    try:
        transcript_list = ytt_api.list(video_id)

        # 1. Try English transcript directly
        try:
            transcript = transcript_list.find_transcript(['en'])
            print("Fetched original English transcript.")
            return transcript.fetch()
        except NoTranscriptFound:
            pass

        # 2. Try any transcript that is translatable to English
        for transcript in transcript_list:
            if transcript.is_translatable and 'en' in [lang['language_code'] for lang in transcript.translation_languages]:
                try:
                    print(f"Fetched and translated from {transcript.language} to English.")
                    return transcript.translate('en').fetch()
                except Exception as e:
                    print(f"Translation failed: {e}")

        # 3. Try Hindi transcript
        try:
            transcript = transcript_list.find_transcript(['hi'])
            print("Fetched original Hindi transcript.")
            return transcript.fetch()
        except NoTranscriptFound:
            pass

        # 4. Fallback to any available transcript
        any_transcript = transcript_list._transcripts[0]
        print(f"Returning available transcript in language: {any_transcript.language}")
        return any_transcript.fetch()

    except TranscriptsDisabled:
        print("Transcripts are disabled for this video.")
        return None
    except Exception as e:
        print(f"Could not retrieve transcript: {e}")
        return None

def youtube_doc(url):
    raw_data = get_best_transcript(url)

    if raw_data:
        # raw_data is a list of {'text', 'start', 'duration'}
        if isinstance(raw_data, FetchedTranscript):
            transcript_pieces = [
                {
                    "text": snippet.text,
                    "start": snippet.start,
                    "duration": snippet.duration,
                }
                for snippet in raw_data.snippets
            ]
        else:
            transcript_pieces: List[Dict[str, Any]] = transcript_object  # type: ignore[no-redef]


        docs=list(
            map(
                lambda transcript_piece: Document(
                    page_content=transcript_piece["text"].strip(" "),
                    metadata=dict(
                        filter(
                            lambda item: item[0] != "text", transcript_piece.items()
                        )
                    ),
                ),
                transcript_pieces,
            )
        )

        # Join all page contents
        combined_content = "\n\n".join(doc.page_content for doc in docs)

        # Assume all documents share the same source (adjust if needed)
        # Option 1: Use the source from the first document
        combined_metadata = docs[0].metadata if docs else {}

        # Create one combined Document
        combined_doc = Document(page_content=combined_content, metadata=combined_metadata)

        # Wrap in list to match your expected format
        final_output = [combined_doc]


        return final_output
    else:
        return None
    


a=youtube_doc("https://youtu.be/c20XsM9BWEM?si=-zgJ9R0QfybZzKi9")
print(a)