import sys, os, base64
import streamlit as st

# Add notebooks folder to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "notebooks")))

from Smart_Rag_Databricks_Chatbot_V2 import rag_answer

# --------------------------------------------------
# 🖼️ Load Background Image (optional)
# --------------------------------------------------
background_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "backround_pic_chatbot.png"))

def get_base64_of_bin_file(bin_file):
    with open(bin_file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

if os.path.exists(background_path):
    bg_image = get_base64_of_bin_file(background_path)
else:
    bg_image = None

# --------------------------------------------------
# 🎨 WhatsApp-style UI CSS
# --------------------------------------------------
if bg_image:
    background_style = f"""
    [data-testid="stAppViewContainer"] {{
        background-image: url("data:image/png;base64,{bg_image}");
        background-size: cover;
        background-position: center;
        background-attachment: fixed;
        background-repeat: no-repeat;
    }}
    """
else:
    background_style = """
    [data-testid="stAppViewContainer"] {
        background: linear-gradient(135deg, #dcf8c6 0%, #ffffff 100%);
    }
    """

st.markdown(f"""
<style>
    {background_style}

    /* Dark overlay to make text readable */
    [data-testid="stAppViewContainer"]::before {{
        content: "";
        position: absolute;
        top: 0; left: 0; right: 0; bottom: 0;
        background: rgba(0, 0, 0, 0.35);   /* darker overlay */
        z-index: 0;
    }}

    [data-testid="stAppViewContainer"] > div {{
        position: relative;
        z-index: 1;
        color: #f1f1f1 !important;  /* make text bright */
    }}

    /* Sidebar */
    [data-testid="stSidebar"] {{
        background: #075E54;
        color: #ffffff;
        border-right: 3px solid #25D366;
    }}

    /* Titles and headings */
    h1, h2, h3 {{
        color: #ffffff;
        text-shadow: 1px 1px 3px rgba(0,0,0,0.6);
    }}

    /* Chat bubbles */
    .user-message {{
        background-color: #dcf8c6;
        color: #000;
    }}
    .bot-message {{
        background-color: #f1f1f1;
        color: #000;
    }}

    /* Input box */
    .stTextInput>div>div>input {{
        border-radius: 25px;
        border: 2px solid #25D366;
        padding: 10px;
        font-size: 16px;
        background-color: rgba(255,255,255,0.95);
        color: #000000;
    }}

    /* Button */
    .stButton>button {{
        background: linear-gradient(to right, #25D366, #128C7E);
        color: #ffffff;
        border-radius: 25px;
        font-size: 16px;
        padding: 8px 25px;
        border: none;
        transition: all 0.3s ease-in-out;
    }}
    .stButton>button:hover {{
        background: linear-gradient(to right, #128C7E, #25D366);
        transform: scale(1.05);
    }}
</style>
""", unsafe_allow_html=True)


# --------------------------------------------------
# 🧱 Streamlit App Config
# --------------------------------------------------
st.set_page_config(page_title="💬 Smart RAG Chatbot", layout="wide")
st.title("💬 Smart RAG Chatbot for Patient Risk Prediction")

st.markdown("""
This chatbot uses:
- 🧠 **Flan-T5** for reasoning  
- 🔍 **OpenSearch** for document retrieval  
- 💾 **Databricks SQL** for live query execution  
""")

# --------------------------------------------------
# 💬 Chat Logic
# --------------------------------------------------
if "history" not in st.session_state:
    st.session_state["history"] = []

question = st.text_input("Ask your question:", placeholder="Type your message...")

if st.button("Send 💬") and question.strip():
    with st.spinner("Thinking... 🤔"):
        try:
            answer = rag_answer(question)
            st.session_state["history"].append(("You", question))
            st.session_state["history"].append(("Bot", answer))
        except Exception as e:
            st.error(f"⚠️ Error: {e}")

# --------------------------------------------------
# 🧾 Display Chat
# --------------------------------------------------
if st.session_state["history"]:
    st.markdown("<div class='chat-container'>", unsafe_allow_html=True)
    for sender, msg in reversed(st.session_state["history"]):
        if sender == "You":
            st.markdown(f"<div class='user-message'>{msg}</div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='bot-message'>{msg}</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

# --------------------------------------------------
# ⚙️ Sidebar Configuration
# --------------------------------------------------
st.sidebar.header("⚙️ Configuration")
st.sidebar.markdown("""
**Backend:**  
- OpenSearch: `localhost:9200`  
- Databricks SQL: `connected`  
- Model: `google/flan-t5-base`  
- Embeddings: `BAAI/bge-small-en`
""")
st.sidebar.success("✅ Ready for queries!")
