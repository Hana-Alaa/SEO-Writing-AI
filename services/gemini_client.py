from google import genai

client = genai.Client(api_key="GEMINI_API_KEY")  

response = client.models.generate_content(
    model="gemini-3-flash-preview",
    contents="اكتب outline لمقال عن SEO writing باستخدام AI",
    temperature=0.7,
    max_output_tokens=1000  
)

print(response.text)


