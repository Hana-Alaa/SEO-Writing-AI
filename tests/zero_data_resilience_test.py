import jinja2
from jinja2 import Environment, FileSystemLoader, StrictUndefined
import os

def test_zero_data_resilience():
    # We use StrictUndefined to FORCE a crash if any variable is accessed nakedly
    env = Environment(
        loader=FileSystemLoader("assets/prompts/templates"),
        undefined=StrictUndefined
    )
    
    templates = [f for f in os.listdir("assets/prompts/templates") if f.endswith(".txt")]
    
    print("☢️ Starting Zero-Data Stress Test (Absolute Resilience Proof)...")
    print("-" * 80)
    print("Goal: Render every template with {} (Empty Context) and catch all StrictUndefined crashes.")
    print("-" * 80)

    success_count = 0
    fail_count = 0

    for t_name in sorted(templates):
        try:
            template = env.get_template(t_name)
            # Render with NOTHING. This will crash if any variable is used as {{ var }} or {{ var.key }}
            # without being protected by a filter or .get()
            template.render({}) 
            print(f"✅ {t_name.ljust(40)} | RESILIENT (Survived Zero Data)")
            success_count += 1
        except jinja2.exceptions.UndefinedError as e:
            print(f"❌ {t_name.ljust(40)} | CRASHED: {e}")
            fail_count += 1
        except Exception as e:
            # Other errors (syntax, etc.) are also considered failures for this test
            print(f"⚠️ {t_name.ljust(40)} | ERROR: {e}")
            fail_count += 1

    print("-" * 80)
    print(f"🏁 RESULTS: {success_count} Resilient, {fail_count} Vulnerable")
    if fail_count == 0:
        print("💎 FINAL VERDICT: The entire template ecosystem is NUCLEAR HARDENED. 👸🛡️✨")
    else:
        print("🛠️ ACTION REQUIRED: Some templates still require .get() or | default() guards.")
    print("-" * 80)

if __name__ == "__main__":
    test_zero_data_resilience()
