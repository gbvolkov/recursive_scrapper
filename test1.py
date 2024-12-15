import asyncio
from playwright.async_api import async_playwright

async def download_image_playwright_async(image_url, download_path):
    async with async_playwright() as p:
        # Launch the browser in headless mode
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        
        # If authentication is required, perform it here
        # Example:
        # page = await context.new_page()
        # await page.goto("https://example.com/login")
        # await page.fill("#username", "your_username")
        # await page.fill("#password", "your_password")
        # await page.click("#login-button")
        # await page.wait_for_load_state("networkidle")
        # Close the login page if necessary
        # await page.close()

        # Make the GET request to download the image
        response = await context.request.get(image_url)
        
        if response.status == 200:
            # Write the image content to the specified path
            with open(download_path, 'wb') as f:
                f.write(await response.body())
            print(f"Image successfully downloaded to: {download_path}")
        else:
            print(f"Failed to download image. Status code: {response.status}")
        
        # Close the browser
        await browser.close()

if __name__ == "__main__":
    image_url = "https://plant-1302037000.cos.na-siliconvalley.myqcloud.com/data//peanut_black_spot_mixed_with_net_blotch/DSC02942.JPG"  # Replace with your image URL
    download_path = "downloaded_image.png"  # Desired download path
    asyncio.run(download_image_playwright_async(image_url, download_path))