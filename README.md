# FC Online Match Dashboard

## &nbsp;&nbsp;&nbsp;Description
- OPEN API 활용 및 자동화 경험(NEXEN OPEN API)
- 공식 경기(랭크전)의 승리 요인 파악 (패스, 슈팅, 골, 드리블, 카드 유무 등)
- 공식 경기(랭크전)의 슛, 골, 패스, 수비 등 매치 정보를 시각적으로 확인
- 데이터 적재 자동화를 통해 실시간 정보 확인 가능

## &nbsp;&nbsp;&nbsp;Framework & Version
- **Crawling** : requests (2.25.1)
- **Data-Processing** : pandas (2.2.2), boto3 (1.34.117), snowflake-connector (3.10.1)

## &nbsp;&nbsp;&nbsp;Infra Architecture
![image](https://github.com/ss721229/FC-Online-Match-Dashboard/assets/53392184/55ff1965-509b-458a-8a10-5c9d3b70f31a)

## &nbsp;&nbsp;&nbsp;ERD
![image](https://github.com/ss721229/FC-Online-Match-Dashboard/assets/53392184/f3866fb4-d26c-44d5-bd6f-350455a9c245)

## &nbsp;&nbsp;&nbsp;EC2 Crontab
- API 데이터는 매 시 정각에 업데이트 되므로 여유 있게 매 시 5분마다 데이터를 수집
- 데이터 수집 이후 S3 파일 COPY 및 analytics 테이블 제작을 진행
``` bash
5 * * * * . $HOME/.bashrc; /usr/bin/python3 /home/ec2-user/FC-Online-Match-Dashboard/scraping.py >> /home/ec2-user/FC-Online-Match-Dashboard/cron.log 2>&1

10 * * * * . $HOME/.bashrc; /usr/bin/python3 /home/ec2-user/FC-Online-Match-Dashboard/COPY.py >> /home/ec2-user/FC-Online-Match-Dashboard/cron.log 2>&1
```

## &nbsp;&nbsp;&nbsp;Dashboard <<a href="https://sanseo.tistory.com/139">결과 분석</a>>
![image](https://github.com/ss721229/FC-Online-Match-Dashboard/assets/53392184/0a6881b7-eb31-44f9-9d6b-6af3931627be)

## &nbsp;&nbsp;&nbsp;Process
- <a href="https://sanseo.tistory.com/117">FC Online 공식 경기 분석 (1) - 계획서</a>
- <a href="https://sanseo.tistory.com/120">FC Online 공식 경기 분석 (2) - S3 버킷 생성 및 스크래핑 코드 작성</a>
- <a href="https://sanseo.tistory.com/123">FC Online 공식 경기 분석 (3) - 인프라 구성(S3, Snowflake, Preset)</a>
- <a href="https://sanseo.tistory.com/124">FC Online 공식 경기 분석 (4) - Snowflake 기본 설정 및 COPY</a>
- <a href="https://sanseo.tistory.com/126">FC Online 공식 경기 분석 (5) - Snowflake analytics 테이블 생성</a>
- <a href="https://sanseo.tistory.com/128">FC Online 공식 경기 분석 (6) - EC2 / crontab 자동화 1</a>
- <a href="https://sanseo.tistory.com/129">FC Online 공식 경기 분석 (7) - EC2 / crontab 자동화 2</a>
- <a href="https://sanseo.tistory.com/133">FC Online 공식 경기 분석 (8) - Preset Dashboard, 자동화</a>
- <a href="https://sanseo.tistory.com/139">FC Online 공식 경기 분석 (9) - 결과 분석</a>
- <a href="https://sanseo.tistory.com/142">FC Online 공식 경기 분석 - 회고

## &nbsp;&nbsp;&nbsp;Good
- EC2 / Crontab을 활용해 자동화 서비스를 성공적으로 구현하였다.
- 민감한 정보(API Key, Snowflake ID/PW 등)을 환경 변수로 지정하여 보안 이슈를 방지하였다.
- 에러가 발생하는지 확인하기 위해 log 파일을 활용하였다.
- Preset Chart 분석으로 공식 경기에서 이기기 위한 방법을 도출하였다.

## &nbsp;&nbsp;&nbsp;Bad
- 에러가 발생했을 때, 알림(Slack 등)을 통해 즉각 조치를 취하도록 하면 좋을 것이다.
- Snowflake trial 기간 이슈로 짧은 기간의 데이터로 분석을 진행하였다.
