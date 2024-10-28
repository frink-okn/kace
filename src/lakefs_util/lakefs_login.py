import aiohttp


async def login_and_get_cookies(lakefs_host, access_key, secret_key):
    # Create a session to store cookies
    async with aiohttp.ClientSession() as session:
        # Send a POST request with login credentials
        async with session.post(lakefs_host + '/api/v1/auth/login', json={"access_key_id": access_key,
                                                                   "secret_access_key": secret_key}) as response:
            if response.status == 200:
                cookies = session.cookie_jar.filter_cookies(lakefs_host + '/auth/login')
                return cookies
            else:
                raise Exception("Lakefs login error")
