import streamlit as st
from streamlit_mic_recorder import speech_to_text



# Initialize session state
if "is_recording" not in st.session_state:
    st.session_state.is_recording = False
if "recorded_text" not in st.session_state:
    st.session_state.recorded_text = ""



# Single toggle button
if not st.session_state.is_recording:
    if st.button("🎙️ Try Voice instead"):
        st.session_state.is_recording = True
        st.rerun()


# Recording section
if st.session_state.is_recording:
    text = speech_to_text(
        language="en",
        just_once=False,
        key="voice_input",
        use_container_width=False
    )
    
    if text:
        st.session_state.recorded_text = text
        st.session_state.is_recording = False
        st.rerun()

# Display recorded text
if st.session_state.recorded_text:
    st.markdown("### Recorded Text:")
    st.write(st.session_state.recorded_text)
