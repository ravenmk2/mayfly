<template>
  <div style="display:grid; grid-template-columns: 1.2fr 1fr; gap:14px">
    <div class="panel">
      <h3>发布消息</h3>
      <el-form label-position="top">
        <el-form-item label="Topic">
          <el-input v-model="form.topic" class="mono" placeholder="sensors/room1/temperature"></el-input>
        </el-form-item>
        <div style="display:flex; gap:14px; align-items:center; flex-wrap:wrap">
          <el-form-item label="QoS" style="width:150px">
            <el-select model-value="0" disabled><el-option value="0" label="QoS 0"></el-option></el-select>
          </el-form-item>
          <el-form-item label="Retained" style="width:90px"><el-switch v-model="form.retain"></el-switch></el-form-item>
          <el-form-item label="Payload 格式" style="width:140px">
            <el-select v-model="form.fmt">
              <el-option value="text" label="Text"></el-option>
              <el-option value="json" label="JSON"></el-option>
              <el-option value="hex" label="Hex"></el-option>
            </el-select>
          </el-form-item>
        </div>
        <el-alert type="info" :closable="false" style="margin-bottom:14px"
          title="服务端内部发布固定为 QoS 0;Hex 格式将以 base64 编码传输"></el-alert>
        <el-form-item label="Payload">
          <el-input v-model="form.payload" type="textarea" :rows="6" class="mono"
                    :placeholder="form.fmt === 'hex' ? '7b 22 76 61 6c 75 65 22 3a 20 32 33 2e 36 7d' : ''"></el-input>
        </el-form-item>
        <el-button type="primary" :icon="Promotion" :loading="sending" @click="doPublish">发 布</el-button>
      </el-form>
    </div>
    <div class="panel">
      <h3>发布记录</h3>
      <el-timeline v-if="history.length">
        <el-timeline-item v-for="(h,i) in history" :key="i" :timestamp="h.time" placement="top">
          <div class="mono" style="font-size:12.5px">{{ h.topic }}
            <el-tag size="small" effect="plain">QoS 0</el-tag>
            <el-tag v-if="h.retain" size="small" type="warning" effect="plain">R</el-tag>
          </div>
          <div class="mono" style="font-size:12px; color:var(--mf-dim); margin-top:4px">{{ h.payload.slice(0, 60) }}</div>
        </el-timeline-item>
      </el-timeline>
      <el-empty v-else description="暂无发布记录" :image-size="70"></el-empty>
    </div>
  </div>
</template>

<script setup>
import { reactive, ref } from 'vue';
import { ElMessage } from 'element-plus';
import { Promotion } from '@element-plus/icons-vue';
import { publish } from '../api/index.js';

const form = reactive({ topic: '', retain: false, fmt: 'text', payload: '' });
const history = ref([]);
const sending = ref(false);

function hexToBase64(hex) {
  const clean = hex.replace(/[^0-9a-fA-F]/g, '');
  if (clean.length % 2 !== 0) throw new Error('hex 长度必须为偶数');
  const bytes = new Uint8Array(clean.length / 2);
  for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  let bin = '';
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin);
}

async function doPublish() {
  if (!form.topic) {
    ElMessage.warning('请输入 Topic');
    return;
  }
  if (form.topic.includes('+') || form.topic.includes('#')) {
    ElMessage.warning('发布 Topic 不能包含通配符');
    return;
  }
  let payload = form.payload;
  let encoding = 'utf8';
  try {
    if (form.fmt === 'json') {
      payload = JSON.stringify(JSON.parse(payload));
    } else if (form.fmt === 'hex') {
      payload = hexToBase64(payload);
      encoding = 'base64';
    }
  } catch (e) {
    ElMessage.error('Payload 格式错误:' + e.message);
    return;
  }
  sending.value = true;
  try {
    await publish(form.topic, payload, encoding, form.retain);
    history.value.unshift({
      topic: form.topic, retain: form.retain, payload: form.payload,
      time: new Date().toLocaleTimeString('zh-CN', { hour12: false }),
    });
    ElMessage.success(`已发布到 ${form.topic}`);
  } catch (e) {
    ElMessage.error(e.message);
  } finally {
    sending.value = false;
  }
}
</script>
