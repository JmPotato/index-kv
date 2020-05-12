package test

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/JmPotato/index-kv/constdef"
	"github.com/JmPotato/index-kv/data"
	"github.com/JmPotato/index-kv/index"
)

var (
	testIndex          *index.Index = &index.Index{}
	keyList, valueList []string
)

func TestHash(t *testing.T) {
	var testIndex *index.Index
	keySet := []string{
		"dhsjakh321",
		"jgfdjfda%(@",
		"SDAD12xghfvjjhfd",
		"FDfdsnjfkdhsbfjk1231312313dsadsadsadsada3213sdsad1232131jk34h323213789yr8^$*&^%*#%TF3213dsafbnhjashdh34n23jnj3fas",
		"AYg^bGQOlsoLNhD$!mWGexms9zyyfHA9^#y3)G$gRLqSLN0wWvc-aRPilO^Ik98up5iuVRvDtGZDcH*FYi0pu*KQrXLS)zs6nBF&vhfZnef5x(X8wdGifQhlUss6)S%9yrIjeYGQy!ttRJg*CZQfh(j6_$$XHMZj073Zq%Dw_*Ua_7wjpsPnCA9Wzvsbi*cM^cu@XaA(Vi!ROul7YOXvbuwdvvQ9$!AXrzLFgKgI2dMitE$Kc4AdMZQIZq9*a9GGb2HNQ!BUltvU^4G5&V2kN_-9CuwtW#%9iVRXU%HH02A&Dc&kvTI#Oqnsf^1iL70$Ljr%soqg!IusL8KSRn^nXJPnGAibs5IYovM46MZoKXPJlU9H!wFzBI3HzXvCcFrPWJCcQ5DqUnxYvhA#b2msb@dWfDKsp)(5h^O-zwnrjFENPorc_DCu*mV3pd^S(ZKLDXKB_CiBiNGotsIa6Y5V-_)mNWKH**orq^OD@lJkIIxoXLQ$7^1ikKz#jEzo8OQ6#kLVUz)JHvR(YKV(38!XNJBqzKrt*^C@Y&!vVAY1apYygu2yvMyFxmY-TZrfp(1^jRzh^!Qz*07uV4!tscGX4y%N_EckMuRP)C7&T&b0kV!AjzXyP!GRgW5NrMZy&qmIzoeD5tsyXP)6%32R_YJhxkSO4sWmMiCPBk07ObuxhI7qEmEz#8jNV9Ri$y",
		"k7)Q33AiJ(7PaB#B0%fFsw1RjK_Enc^kTaNF)C$a0_7XXw(0QmoDEcuRjRpuRWkbKi8S7jLwoL!aUt)x-%1owM_68_1niEcj%k130)9W_4_lvL1tQ-u)S1UzTWm0VmP9sA_zNSGkfaw3_@mPfeAYg^bGQOlsoLNhD$!mWGexms9zyyfHA9^#y3)G$gRLqSLN0wWvc-aRPilO^Ik98up5iuVRvDtGZDcH*FYi0pu*KQrXLS)zs6nBF&vhfZnef5x(X8wdGifQhlUss6)S%9yrIjeYGQy!ttRJg*CZQfh(j6_$$XHMZj073Zq%Dw_*Ua_7wjpsPnCA9Wzvsbi*cM^cu@XaA(Vi!ROul7YOXvbuwdvvQ9$!AXrzLFgKgI2dMitE$Kc4AdMZQIZq9*a9GGb2HNQ!BUltvU^4G5&V2kN_-9CuwtW#%9iVRXU%HH02A&Dc&kvTI#Oqnsf^1iL70$Ljr%soqg!IusL8KSRn^nXJPnGAibs5IYovM46MZoKXPJlU9H!wFzBI3HzXvCcFrPWJCcQ5DqUnxYvhA#b2msb@dWfDKsp)(5h^O-zwnrjFENPorc_DCu*mV3pd^S(ZKLDXKB_CiBiNGotsIa6Y5V-_)mNWKH**orq^OD@lJkIIxoXLQ$7^1ikKz#jEzo8OQ6#kLVUz)JHvR(YKV(38!XNJBqzKrt*^C@Y&!vVAY1apYygu2yvMyFxmY-TZrfp(1^jRzh^!Qz*07uV4!tscGX4y%N_EckMuRP)C7&T&b0kV!AjzXyP!GRgW5NrMZy&qmIzoeD5tsyXP)6%32R_YJhxkSO4sWmMiCPBk07ObuxhI7qEmEz#8jNV9Ri$ybCQ7YEhuyYfbP9LvMsQyp7PNJQV*s2UAxFKmg7**7_bjEa27U5^2Tv^LaWAaVb3e_J1ByvEsUWbO7#_Iio-iBKdky#0m-y4Ea0U00P-1DqY$k1ypfJOh2eFdqRu&E(1v3u$H&)Y7WjTDUid_LpK7l3YKVPLocuuJXL75%#-DtbrjXfL9d(%(4q$0qzKT4i-kTqs2n6)kM4oriouSIgeQBmfM5wy(Lx(CBKwyV&16vGvO&l0cL!YaEXHwxfuh$EX%o3a#PSUiQU4z255l8#1E(s1MT0_9n$L7BPBL#!-&Q4ed@@^W2Tq!ehr9pMEf6oi^3_4SM0pnpe(rr4HlRDBXhmY6qQ8W-aU4efSKy5@sMnu8O7P%wu(hFL*JfCfAeYgMWcGHdWZYHgHHgQk2Kj6JeGSiT6UX-4qDccH5w3$#9)$KXKt@ldal3AE_47oTz0P2hZSv9kl4Z!Z@cJyBJZKYfJbxt9^RrGRqCB35KK4ixDm%am2IBb(mRRXmleI%6Jt3#b_tQC3s*VbaMc4rtchYpiI4ZAvO#YvnnH7$RaGK^h(7m_lKS-ahhGsI2$TAvzjv8sFTik**V3NCgRA%ll0m*hqdE_SsA9gmsmo4gpl1mwwBoL-l0UE8-#xA9l9Ms@@#y7H3$fmHchpoBLn0wv432X@EyL7gmNU)q%K84IZr4VcjPN^yv2_9uz$r5NkRMGTuQ63FFGg7fHDzcB%z^ALcz(ofAX77N3IfAf!jW4-Xk#&tZvuhF#@%rnYDYNSp#-WD)#6R3QZU#Dnj)vxwE6YxnJ7)RGs5jpnfHp4-m$*)N$SDG!)I(sbmeXM%7t4mj9^NY$xA3NqOS2lOqafy_K(_-PG)c^PGLILPBlcu5upLXnocc_Z$e)umaMGIF1kJuDxfEg_aB-N#iR$0h@NTotAp!k%B(G@-gJF&se$)I*_fI-32_TZ%gvRwZl(^Lva2_ov-iR^nSkwii4dDhgd^Or#nUB^l#!7I1jSuBB7OYcVA@78RfXoWo_sTQtQSm5OU2%Cb9RAmXkfMgu1uElPMkYcs&!jIk6aqOs2i0agqId&oN)w-*HRAJW$_m#%qMHJN_c5BN12MBMKOxX@5z!vwOA5S*EZCwfW&cJI(eDIs^HMgxd&gn(DIRIN*grd%@*F50y1#z@Q@@G0BPhNacGRTeF$WlUluK2*o3&lzQUQJALgO9LNXvHhRg(rOgZ-(^fs7QVKWfveCq_@kr7X$sOd0yrE^eTo9u*-w)(XgFZ)6*68**he@X%9%u_jspPH(!XR-jZAiQDWbfuod&$S7lX*C3YEVqtfP3qQrE1jkw$hHY2X%%N9dwIRBJp2knBjaQdK_GErVky5sw!td7lFmN99$sEz(E4U@f^R44Z(u#d&r#VduKn%U&e6@430W1FF8MQN4vAd!YemEOd^QLmeQzZKZ9B3VsDeqyytAN(X)qg5VnC@30W-m%9t0ONmQdKo8mDV2961IG9&a8apX_87uztsPUele$Waa!f-&atUI7n&KTvpbF#AgnLv@z5%NU@@6hN6_ehO6)m8vo4yT(xEkTMSGElEA@c$0yVVH0lX4)X!1JMCoxLyXcIzYfn528TRs4F0P@%GW2)4jnM1a8DyPqVejJDVAqRmjMd6m6T_BKxu07nu0!7o-Uyph*%vRhahneLhHRCnoR_)#ZBUXLNtn^QFMpv$cwH(k2-0TSjecjPS9mGWC#%7x^9(pzZYFeQJ@9BvbWB6cia6hbtRM!7n80r(bVYJc*@%5y)wMwf!SkpU2px1Q7g61fR@DSZNhiJt03fgi1dO!K^tt30E0-U$gw!YV3%0TZ$1*C%9Y58mCFIrU$3#_qaORa3@ZagwTcBeB_wLwzZO5g4X2qzuOdMaBmEMFKQr(ZyoRBiDH(u3VNki1yFUicj!&NG-Rd$*L)#22&F2wFHmM04A)xI%1F$siTm3AEy-@s9CpE-f0Slq",
	}
	chunkIDSet := []uint16{59095, 42321, 41998, 25468, 58670, 14670}

	for i, key := range keySet {
		assertEqual(t, uint16(testIndex.Hash([]byte(key))%constdef.CHUNK_SIZE), chunkIDSet[i], fmt.Sprintf("Wrong Hash result for key=%s and chunkID=%d", key, chunkIDSet[i]))
	}
}

func TestIndexCreate(t *testing.T) {
	keyList, valueList = data.GenerateRandomData()
	testIndex.New(constdef.DATA_FILENAME)
}

func TestIndexSingleGet(t *testing.T) {
	clearChunks()

	if fileExist(constdef.DATA_FILENAME) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			testIndex.New(constdef.DATA_FILENAME)
			wg.Done()
		}()
		wg.Add(1)
		go func() {
			dataFile, err := os.Open(constdef.DATA_FILENAME)
			errorHandle(err)
			keyList, valueList = data.ReadKV(dataFile)
			wg.Done()
		}()
		wg.Wait()
	} else {
		keyList, valueList = data.GenerateRandomData()
		testIndex.New(constdef.DATA_FILENAME)
	}

	for i, key := range keyList {
		valueRead := testIndex.Get(key)
		assertEqual(t, valueList[i], valueRead, fmt.Sprintf("Mismatch for keyList[%d]", i))
	}
}

func TestIndexMutiGet(t *testing.T) {
	clearChunks()

	if fileExist(constdef.DATA_FILENAME) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			testIndex.New(constdef.DATA_FILENAME)
			wg.Done()
		}()
		wg.Add(1)
		go func() {
			dataFile, err := os.Open(constdef.DATA_FILENAME)
			errorHandle(err)
			keyList, valueList = data.ReadKV(dataFile)
			wg.Done()
		}()
		wg.Wait()
	} else {
		keyList, valueList = data.GenerateRandomData()
		testIndex.New(constdef.DATA_FILENAME)
	}

	valueListRead := testIndex.MGet(&keyList)
	for idx, valueRead := range *valueListRead {
		chunkID := testIndex.Hash([]byte(keyList[idx])) % constdef.CHUNK_SIZE
		assertEqual(t, valueList[idx], valueRead, fmt.Sprintf("Mismatch for keyList[%d], chunkID=%d", idx, chunkID))
	}
}
