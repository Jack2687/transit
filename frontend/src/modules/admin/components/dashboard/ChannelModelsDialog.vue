<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue'
import { useEventListener } from '@vueuse/core'
import { useI18n } from 'vue-i18n'
import { Loader2, X } from 'lucide-vue-next'
import { saveChannelModels } from '../../api/connectionHealth'
import { connectionHealthMessageKey } from '../../composables/useConnectionHealth'
import type { AdminGroupAccount } from '../../types/connectionHealth'

const props = defineProps<{ account: AdminGroupAccount }>()
const emit = defineEmits<{ close: []; saved: [models: { targetId: string; probeModel: string; qualityModel: string; qualityBaseUrl: string }] }>()
const { t, te } = useI18n()
const p = 'admin.connectionHealth.channelModels'
const inheritProbe = ref(!props.account.probeModel)
const inheritQuality = ref(!props.account.qualityModel)
const probeModel = ref(props.account.probeModel || '')
const qualityModel = ref(props.account.qualityModel || props.account.qualityGlobalModel || '')
const qualityBaseUrl = ref(props.account.qualityBaseUrl || '')
const busy = ref(false)
const error = ref('')
const dialog = ref<HTMLElement | null>(null)
const previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null
const valid = computed(() => (inheritProbe.value || probeModel.value.trim().length > 0) && (inheritQuality.value || qualityModel.value.trim().length > 0))
const close = () => { if (!busy.value) emit('close') }
onMounted(async () => { await nextTick(); dialog.value?.focus() })
onUnmounted(() => previousFocus?.focus())
useEventListener(document, 'keydown', event => {
  if (event.key === 'Escape') { event.preventDefault(); close() }
  if (event.key !== 'Tab' || !dialog.value) return
  const fields = Array.from(dialog.value.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled)'))
  const first = fields[0], last = fields.at(-1)
  const outside = !dialog.value.contains(document.activeElement) || document.activeElement === dialog.value
  if (event.shiftKey && (outside || document.activeElement === first)) { event.preventDefault(); last?.focus() }
  else if (!event.shiftKey && (outside || document.activeElement === last)) { event.preventDefault(); first?.focus() }
})
async function save() {
  if (busy.value || !valid.value) return
  busy.value = true; error.value = ''
  try {
    const saved = await saveChannelModels(props.account.targetId, {
      probeModel: inheritProbe.value ? '' : probeModel.value.trim(),
      qualityModel: inheritQuality.value ? '' : qualityModel.value.trim(),
      qualityBaseUrl: qualityBaseUrl.value.trim(),
    })
    emit('saved', saved)
    emit('close')
  } catch (err) {
    error.value = t(connectionHealthMessageKey(err instanceof Error ? err.message : 'admin.connectionHealth.errors.request', te))
  } finally { busy.value = false }
}
</script>

<template>
  <Teleport to="body">
    <div class="fixed inset-0 z-[150] flex items-center justify-center bg-background/60 p-4 backdrop-blur-sm" @click.self="close">
      <form ref="dialog" role="dialog" aria-modal="true" :aria-label="t(`${p}.title`)" tabindex="-1" class="max-h-[calc(100dvh-2rem)] w-full max-w-lg space-y-5 overflow-y-auto rounded-xl border border-border bg-card p-5 text-foreground shadow-xl outline-none" @submit.prevent="save">
        <div class="flex items-start justify-between gap-3">
          <div class="min-w-0"><h3 class="text-base font-semibold">{{ t(`${p}.title`) }}</h3><p class="mt-1 truncate text-sm text-muted-foreground">{{ account.name || account.id }}</p></div>
          <button type="button" :disabled="busy" :aria-label="t(`${p}.close`)" class="rounded p-1 hover:bg-surface" @click="close"><X class="h-4 w-4" /></button>
        </div>
        <fieldset :disabled="busy" class="space-y-3 rounded-lg border border-border/60 p-3">
          <legend class="px-1 text-sm font-medium">{{ t(`${p}.probe`) }}</legend>
          <label class="flex items-center gap-2 text-sm"><input v-model="inheritProbe" type="checkbox" class="accent-primary" />{{ t(`${p}.inheritProbe`) }}</label>
          <label v-if="!inheritProbe" class="block space-y-1 text-xs text-muted-foreground"><span>{{ t(`${p}.name`) }}</span><input v-model="probeModel" required maxlength="200" autocomplete="off" :placeholder="t(`${p}.placeholder`)" class="w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground" /></label>
          <p class="text-xs leading-5 text-muted-foreground">{{ t(`${p}.probeHint`) }}</p>
        </fieldset>
        <fieldset :disabled="busy" class="space-y-3 rounded-lg border border-border/60 p-3">
          <legend class="px-1 text-sm font-medium">{{ t(`${p}.quality`) }}</legend>
          <label class="flex items-center gap-2 text-sm"><input v-model="inheritQuality" type="checkbox" class="accent-primary" />{{ t(`${p}.inheritQuality`) }}</label>
          <p v-if="inheritQuality && account.qualityGlobalModel" class="break-all text-xs text-muted-foreground">{{ t(`${p}.globalModel`, { model: account.qualityGlobalModel }) }}</p>
          <label v-if="!inheritQuality" class="block space-y-1 text-xs text-muted-foreground"><span>{{ t(`${p}.name`) }}</span><input v-model="qualityModel" required maxlength="200" autocomplete="off" :placeholder="t(`${p}.placeholder`)" class="w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground" /></label>
          <p class="text-xs leading-5 text-muted-foreground">{{ t(`${p}.qualityHint`) }}</p>
          <label class="block space-y-1 text-xs text-muted-foreground"><span>{{ t(`${p}.qualityBaseUrl`) }}</span><input v-model="qualityBaseUrl" type="url" maxlength="2048" autocomplete="off" placeholder="https://api.example.com/v1" class="w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground" /></label>
          <p class="text-xs leading-5 text-muted-foreground">{{ t(`${p}.qualityBaseUrlHint`) }}</p>
        </fieldset>
        <p class="text-xs leading-5 text-muted-foreground">{{ t(`${p}.sharedHint`) }}</p>
        <p v-if="error" role="alert" class="text-sm text-red-600 dark:text-red-400">{{ error }}</p>
        <div class="flex justify-end gap-2">
          <button type="button" :disabled="busy" class="rounded-md border border-border px-3 py-2 text-sm disabled:opacity-50" @click="close">{{ t(`${p}.cancel`) }}</button>
          <button type="submit" :disabled="busy || !valid" class="inline-flex items-center gap-2 rounded-md bg-primary px-3 py-2 text-sm text-primary-foreground disabled:opacity-50"><Loader2 v-if="busy" class="h-4 w-4 animate-spin" />{{ t(`${p}.save`) }}</button>
        </div>
      </form>
    </div>
  </Teleport>
</template>
