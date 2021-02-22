USE Pokemon;
 
#1 잡은 포켓몬이 3마리 이상인 트레이너의 이름을
#잡은 포켓몬 수가 많은 순서대로 출력하시오.

Select name
from Trainer T, CatchedPokemon C
Where T.id = C.owner_id
group by C.owner_id
Having count(*) > 2
order by Count(*) desc
;


#2. 전체 포켓몬 중 가장 많은 2개의 타입을 가진 포켓몬
#이름을 사전순으로 출력하시오.

select P1.name
from Pokemon P1, (
			SELECT type
			FROM Pokemon
			GROUP BY type
			order by count(*) desc, type limit 2
		) as Pokemontoptwo
where P1.type = Pokemontoptwo.type
order by P1.name
;


#3. o가 두번째에 들어가는 포켓몬의 이름을
#사전순으로 출력하시오


select name
from Pokemon
where name LIKE '_o%'
order by name
;

#4. 잡은 포켓몬 중 레벨이 50이상인 포켓몬의
#닉네임을 사전순으로 출력하시오


select nickname
from CatchedPokemon
where level >= 50
order by nickname
;

#5. 이름이 6글자인 포켓몬의 이름을
#사전순으로 출력하시오

select name
from Pokemon
where name LIKE '______'
order by name
;


#6. Blue City 출신의 트레이너를
#사전순으로 출력하시오

select T.name
from Trainer T, City C
where T.hometown = C.name
AND	T.hometown = 'Blue City'
order by T.name
;


#7.Trainer의 고향 이름을 중복 없이
#사전순서대로 출력하세요


select distinct hometown
from Trainer
order by hometown
;

#8. Red가 잡은 포멧몬의 평균 레벨을 출력하세요

select AVG(level)
from Trainer T, CatchedPokemon C
where T.id = C.owner_id AND T.name = 'Red'
group by T.name
;

#9. 잡은 포켓몬중 닉네임이 T로 시작하지 않는
#포켓몬의 닉네임을 사전순으로 출력하세요

select nickname
from CatchedPokemon
where nickname not LIKE 'T%'
order by nickname
;

#10. 잡은 포켓몬 중 레벨이 50이상이고
#owner_id가 6이상인 포켓몬의 닉네임을
#사전순으로 출력하세요.


select nickname
from CatchedPokemon
where owner_id>=6 AND level >= 50
order by nickname
;


#11.포켓몬 도감에 있는 모든 포켓몬의 ID와이름을
#id에 오름차순으로 정렬해 출력하시오

select id,name
from Pokemon
order by id
;

#12. 레벨이 50이하인 잡힌 포켓몬의 닉네임을
#레벨에 오름차순으로 정렬해 출력하시오

select nickname
from CatchedPokemon
where level<=50
order by level
;

#13. 상록시티출신 트레이너가 가진
#포켓몬들의 이름과 포켓몬ID를
#포켓몬ID의 오름차순으로 정렬해 출력하세요

select P.name, P.id
from Trainer T,CatchedPokemon C, Pokemon P
where T.id = C.owner_id AND C.pid=P.id AND T.hometown = 'Sangnok City'
order by P.id
;

#14.모든각도시의 관장이가진 포켓몬들중 
#물포켓몬들의별명을
#별명에 오름차순으로정렬해 출력하세요


select C.nickname
from Trainer T, Gym G, CatchedPokemon C,Pokemon P
where T.id = G.leader_id AND T.id=C.owner_id AND C.pid = P.id AND P.type = 'Water'
order by C.nickname
;


#15. 모든 잡힌 포켓몬들 중에서 진화가 가능한
#포켓몬의 총 수를 출력하시오.

select COUNT(*)
from CatchedPokemon C, Pokemon P, Evolution E
where C.pid = P.id AND P.id = E.before_id
;

#16. 포켓몬 도감에 있는 포켓몬 중
#water, electric, psychic 타입 포켓몬의
#총합을 출력하시오


select COUNT(*)
from Pokemon
where type = 'Water' or type ='Electric' or type = 'Psychic'
;


#17. 상록시티 출신 트레이너들이 가지고 있는
#포켓몬 종류의 갯수를 출력하시오

select COUNT(DISTINCT pid)
from Trainer T, CatchedPokemon C
where T.id = C.owner_id and T.hometown = 'Sangnok City'
;


#18. 상록시티 출신 트레이너들이 가지고 있는 포켓몬 중
#레벨이 가장 높은 포켓몬의 레벨을 출력하시오


select C.level
from Trainer T,CatchedPokemon C
where T.id = C.owner_id and T.hometown = 'Sangnok City'
order by C.level desc
limit 1
;

#19. 상록시티의 리더가 가지고 있는
#포켓몬 타입의 갯수를 출력하시오


select COUNT(DISTINCT P.type)
from Trainer T, Gym G, CatchedPokemon C, Pokemon P
WHERE T.id = G.leader_id and T.id = C.owner_id and C.pid= P.id and T.hometown = 'Sangnok City'
;

#20. 상록시티 출신 트레이너의 이름과
#각 트레이너가 잡은 포켓몬 수를
#잡은 포켓몬 수에 대해오름차순으로 출력하시오.


select T.name,COUNT(*)
from Trainer T, CatchedPokemon C
where T.id = C.owner_id and T.hometown = 'Sangnok City'
group by T.name
order by COUNT(*)
;


#21. 포켓몬의 이름이 알파벳 모음으로 시작하는
#포켓몬의 이름을 출력하세요.

select name
from Pokemon
where name LIKE 'A%'
	or name LIKE'E%'
	or name LIKE 'I%'
	or name LIKE'O%'
	or name LIKE 'U%'
;

#22. 포켓몬의 타입과 해당 타입을 가지는 포켓몬의 수가
#몇 마리인지를 출력하세요.
#수에 대해 오름차순(같은 수라면 타입 명의 사전식 오름차순)


select type,Count(*)
from Pokemon
group by type
order by Count(*),type
;

#23. 잡힌 포켓몬 중 레벨이 10이하인 포켓몬을
#잡은 트레이너의 이름을 중복없이 사전순으로 출력


select DISTINCT T.name
from Trainer T,CatchedPokemon C
where T.id = C.owner_id and C.level <=10 
order by T.name
;

#24.각 시티의 이름과 해당 시티의 고향 출신 트레이너들이
#잡은 포켓몬들의 평균 레벨을
#평균 레벨의 오름차순으로 정렬해서 출력하세요

select T.hometown,AVG(C.level)
from Trainer T, CatchedPokemon C
where T.id = C.owner_id
group by T.hometown
order by AVG(C.level)
;


#25. 상록시티 출신 트레이너와 브라운 시티 출신 트레이너가
#공통으로 잡은 포켓몬의 이름을 사전순으로 출력(중복은 제거할것)

select Distinct P.name
from Trainer T, CatchedPokemon C , Pokemon P
where T.id=C.owner_id and T.hometown = 'Sangnok City' and C.pid = P.id and C.pid IN (

				select C1.pid
				from Trainer T1, CatchedPokemon C1
				where T1.id = C1.owner_id and T1.hometown = 'Brown City'
			)
order by P.name
;

#26. 잡힌 포켓몬 중 닉네임에 공백이 들어가는
#포켓몬의 이름을 사전식 내림차순으로 출력

select P.name
from CatchedPokemon C,Pokemon P
where C.pid = P.id and  C.nickname LIKE '% %'
order by P.name desc
;

#27. 포켓몬을 4마리 이상 잡은 트레이너의 이름과
#해당 트레이너가 잡은 포켓몬 중 가장 레벨이 높은포멧몬의 레벨을
#트레이너의 이름에 사전순으로 정렬해서 출력


select T.name, Maxlevel.mxlevel
from Trainer T, CatchedPokemon C, 
	 (
		select T1.name, max(C1.level) as mxlevel
		from Trainer T1, CatchedPokemon C1
		where T1.id=C1.owner_id
		group by T1.name
	 ) as Maxlevel
where T.id= C.owner_id and Maxlevel.name = T.name
group by T.name
HAVING count(*) > 3
order by T.name
;

#28. normal 또는 electric 타입의 포켓몬을 잡은
#트레이너의 이름과 해당 포켓몬의 평균 레벨을
#구한 평균 레벨에 오름차순으로 정렬해서 출력

select T.name, avg(C.level)
from Trainer T, CatchedPokemon C, Pokemon P
where T.id=C.owner_id and C.pid = P.id and ( P.type = 'Normal' or P.type = 'Electric')
group by T.name
order by AVG(C.level)
;

#29. 152번 포켓몬의 이름과 이를 찹은 트레이너의 이름,
#출신 도시의 설명을 잡힌 포켓몬의레벨에
#대한 내림차순 순서대로 출력하세요.

select P.name, T.name, Ci.description
from Trainer T, CatchedPokemon C, City Ci, Pokemon P
where T.hometown = Ci.name and T.id = C.owner_id and C.pid = 152 and C.pid = P.id
order by C.level desc
;

#30. 포켓몬 중 3단 진화가 가능한 포켓몬의 ID와
#해당 포켓몬의 이름을 1단 진화 형태 포켓몬의 이름,
#2단 진화 형태 포켓몬의 이름, 3단 진화 포켓몬의 이름을
#id의 오름차순으로 출력하시오.

select P.id, P.name, E222.name, E333.name
from Pokemon P, Evolution E1,
	 (
	 	select P1.name, E2.before_id,E2.after_id
		from Pokemon P1,Evolution E2
		where P1.id = E2.before_id
		order by P1.id
	 ) as E222
	 ,
	 (
	  	select P3.name, E3.after_id
		from Pokemon P3, Evolution E3
		where P3.id= E3.after_id
		order by P3.id
	 ) as E333
where P.id = E1.before_id and E1.after_id = E222.before_id and E222.after_id = E333.after_id
order by P.id
;


#31. 포켓몬 ID가 두자리수인 포켓몬의 이름을 사전순으로 출력하시오


select name
from Pokemon
where id >9 and id<100
order by name
;


#32. 어느 트레이너에게도 잡히지 않은 포켓몬의 이름을 사전순으로 출력하시오


select name
from Pokemon
where id not in (
			Select pid
			from CatchedPokemon
		)
order by name
;

#33. 트레이너 Matis가 잡은 포켓몬들의 레벨의 총합을 출력하시오

select SUM(C.level)
from Trainer T, CatchedPokemon C
where T.id = C.owner_id and T.name = 'Matis'
;

#34. 체육관 관장들의 이름을 사전 순으로 출력하세요


select T.name
from Trainer T, Gym G
where T.id = G.leader_id
order by T.name
;


#35. 가장 많은 비율의 포켓몬 타입과 그  비율을 백분율로 출력하시오


select P.type, count(*) / P2.sumofall * 100
from Pokemon P, 
	 (
	  	select count(*) as sumofall
		from Pokemon
	 ) as P2
group by P.type
order by count(*) desc
limit 1
;

#36. 닉네임이 A로 시작하는 포켓몬을 잡은 트레이너의 이름을 사전 순으로 출력하세요


select T.name
from Trainer T, CatchedPokemon C
where T.id = C.owner_id and C.nickname LIKE 'A%'
order by T.name
;

#37. 잡은 포켓몬 레벨의 총합이 가장 높은 트레이너의 이름과 그 총합을 출력세요


select T.name, sum(C.level)
From Trainer T, CatchedPokemon C
where T.id = C.owner_id
group by T.name
order by sum(C.level) desc
limit 1
;

#38. 진화 2단계 포켓몬들의 이름을 사전순으로 출력하세요

select P.name
from Pokemon P, Evolution E1
where P.id = E1.after_id and E1.before_id <> ALL(
			select E2.after_id
			from Evolution E2
		)
order by P.name
;


#39. 동일한 포켓몬을 두 마리 이상 잡은 트레이너의 이름을 사전 순으로 출력하세요

select T.name
from Trainer T, CatchedPokemon C
where T.id = C.owner_id
group by T.name,C.pid
Having count(*) > 1
order by T.name
;


#40. 출신 도시명과, 각 출신 도시별로 트레이너가 잡은
#가장 레벨이 높은 포켓몬의 별명을 출신 도시명의 사전 순으로
#출력하시오

select T1.hometown, C1.nickname
from Trainer T1, CatchedPokemon C1,
	 (
	 	select T.hometown, MAX(C.level) as maxlevel
		from Trainer T, CatchedPokemon C
		where T.id = C.owner_id
		group by T.hometown
	 ) as cityandmx
where T1.id = C1.owner_id and T1.hometown  = cityandmx.hometown and C1.level = cityandmx.maxlevel
order by T1.hometown
;


