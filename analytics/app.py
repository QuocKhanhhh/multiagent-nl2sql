import streamlit as st
import requests
import os

API_URL = os.getenv("API_URL", "http://localhost:8002/ask")

st.set_page_config(page_title="AI-driven Analytics", layout="wide")
st.title("📊 AI-driven Analytics Demo")

question = st.text_input("Hỏi dữ liệu của bạn (tiếng Việt hoặc English):")

if st.button("Phân tích"):
    if question.strip():
        with st.spinner("Đang phân tích..."):
            response = requests.post(API_URL, json={"question": question})
            if response.status_code == 200:
                data = response.json()
                st.subheader("🔎 SQL sinh ra")
                st.code(data.get("sql", ""), language="sql")

                st.subheader("📄 Kết quả raw")
                st.write(data.get("raw_result", []))

                st.subheader("📝 Phân tích")
                st.write(data.get("analysis", ""))
            else:
                st.error(f"Lỗi khi gọi API: {response.text}")
