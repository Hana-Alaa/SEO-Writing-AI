import sys
import os
import unittest
from unittest.mock import patch, MagicMock
import base64

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.image_service import StabilityImageService

class TestStabilityImageService(unittest.TestCase):
    def setUp(self):
        self.service = StabilityImageService(save_dir="output/test_images", api_key="sk-dummy")

    @patch("requests.post")
    @patch("PIL.Image.open")
    def test_full_flow(self, mock_image_open, mock_post):
        # 1. Mock Stability API Response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "artifacts": [
                {"base64": base64.b64encode(b"fake_image_data").decode("utf-8")}
            ]
        }
        mock_post.return_value = mock_response

        # 2. Mock PIL Image for resizing
        mock_img = MagicMock()
        mock_img.width = 1024
        mock_img.height = 1024
        mock_image_open.return_value.__enter__.return_value = mock_img

        # 3. Test Prompt Generation
        outline = [{"id": "sec1", "title": "Intro"}]
        seo_meta = {"main_keyword": "SEO"}
        prompts = self.service.generate_image_prompts_only(outline, seo_meta)
        self.assertEqual(len(prompts), 7)
        self.assertEqual(prompts[0]["image_type"], "Featured Image")

        # 4. Test Download and Process
        test_prompts = [prompts[0]]
        try:
            results = self.service.download_and_process_images(test_prompts)
            
            self.assertEqual(len(results), 1)
            # Use os.path.normpath to handle Windows/Unix path differences in test
            expected_path = os.path.normpath("output/test_images/sec1.png")
            actual_path = os.path.normpath(results[0]["local_path"])
            self.assertEqual(actual_path, expected_path)
            self.assertTrue(mock_post.called)
            self.assertTrue(mock_image_open.called)
        except Exception as e:
            print(f"\nTEST ERROR: {e}")
            import traceback
            traceback.print_exc()
            raise e

if __name__ == "__main__":
    unittest.main()
