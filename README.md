README

### 팀 이름 `t2`
#### 팀원 `정미은` `이정훈` `김태영`
***
#### 프로젝트 주제

영화박스오피스 데이터 수집(Extract)/처리(Transform)/보관 및 활용(Load)을 위한 AIRFLOW DAGS 기능 개발

#### 프로젝트 내용

영화 박스오피스 데이터 수집/처리/보관 및 활용에 대하여 
각 단계에 대한 파이썬 프로그램을 package(PIP 설치) 단위로  개발
개발 package를 airflow 적용 및 운영

#### 특별 미션
 
모든 PIP package에 Ice_breaking 함수를 생성하여 이를 호출하면 
아스키아트로 변환된 팀원 사진이 호출
***

#### 브랜치 전략

- main - release/d1.0.0 - dev/d1.0.0 - d1.0.0/<기능이름>
- mian - release/d2.0.0 - dev/d2.0.0 - d2.0.0/<기능이름>

- 개발 단계
    1. d3.0.0/merge를 dev/d3.0.0/에 하위 브랜치 생성
    2. 기능개발, 파이테스트 통과 후 문제없으면 dev/d3.0.0으로 PR 후 머지
    3. 합친 후에 테스트 진행, 에러 없으면 release/d3.0.0으로 머지

# Extract

![LGTM](https://i.lgtm.fun/2tae.png)

# Version
- v1.0.0
    - ice breacking 함수 생성 
	- ice breacking 테스트 ✅
	- req 테스트 ✅
	- url 테스트 ✅
	- Dataframe cols 테스트 ✅

- v2.0.0
    - parqeut 파일 생성 테스트 ✅
    - year, month, date 형식으로 partitioning ✅
    - 멱등성 유지 기능 테스트 ✅

- v3.0.0
    - 월별 관객수 등수 출력 테스트 ✅
    - year. month 형식으로 partitioning ✅
    - 멱등성 유지 기능 테스트 ✅

# Member
- [정미은](https://github.com/orgs/DE32-Team-Two/people/hahahellooo)
- [김태영](https://github.com/orgs/DE32-Team-Two/people/tbongkim03)
- [이정훈](https://github.com/orgs/DE32-Team-Two/people/Jeonghoon2)

