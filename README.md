# Prefix tree based searchable encryption

This backend implementation stores prefixes in the Prefix Tree and related documents as binary data.

> This code is deployed at `https://se-uavs-demo.lilikovych.name`.

* Set up `.env` file using `example.env`. Create dummy root document in the Prefix Tree collection: `{'hash':BinObj(''), children:[]}`. Put ID of this object to `MONGODB_PREFIXTREE_ROOT_ID` variable
* Install dependencies: `pip3 install -r requirements.txt`. Please note that Uvicorn may require `uvicorn[standard]` installation, [see here](https://www.uvicorn.org/#quickstart).
* Use Uvicorn to start server: `python3 -m uvicorn main:app --port 3000 --host 127.0.0.1`
* Set up the [front-end part](https://github.com/SerhiiKarazinUni/diploma-uavs-frontend) from and enjoy