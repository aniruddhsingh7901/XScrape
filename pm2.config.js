module.exports = {
  apps: [

    // {
    //   name: "public-http",
    //   script: "scripts/public_http_server.py",
    //   interpreter: "python3",
    //   args: "",
    //   cwd: __dirname,
    //   autorestart: true,
    //   watch: false,
    //   time: true,
    //   mergeLogs: true,
    //   out_file: "logs/public_http.out.log",
    //   error_file: "logs/public_http.err.log",
    //   env: {
    //     PYTHONUNBUFFERED: "1",
    //     PORT: "8000",
    //     DIR: "scripts/public_db"
    //   }
    // },
    {
      name: "twitter-scraper",
      script: "/bin/bash",
      // Loop:
      // 1) Refresh twitter targets/trending from gravity + pool sampling
      // 2) Run batch-targets using scripts/cache/twitter_targets.json
      // 3) Sleep and repeat
      args: "-lc 'while true; do \
python3 twitter_suite/cli.py trends --pool scripts/cache/twscrape_refresh.db --max-labels 120 --per-query-limit 100 --trending-max-out 300; \
python3 twitter_suite/cli.py batch-targets --pool scripts/cache/twscrape_refresh.db --targets-json scripts/cache/twitter_targets.json --limit-per-query 100 --db SqliteMinerStorage.sqlite --verbose; \
sleep 60; done'",
      cwd: __dirname,
      autorestart: true,
      watch: false,
      time: true,
      mergeLogs: true,
      out_file: "logs/twitter_scraper.out.log",
      error_file: "logs/twitter_scraper.err.log",
      env: {
        PYTHONUNBUFFERED: "1"
      }
    }
  ]
};
