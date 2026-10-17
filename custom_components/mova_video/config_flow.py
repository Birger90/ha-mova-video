"""Config flow: pick the dreame_vacuum robot to add live video to."""

from __future__ import annotations

from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import callback

from .const import CONF_DREAME_ENTRY, DOMAIN, DREAME_DOMAIN

CONF_HANDSHAKE = "device_handshake"


class MovaVideoConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        entries = {e.entry_id: e.title for e in self.hass.config_entries.async_entries(DREAME_DOMAIN)}
        if not entries:
            return self.async_abort(reason="no_dreame")
        if user_input is not None:
            await self.async_set_unique_id(user_input[CONF_DREAME_ENTRY])
            self._abort_if_unique_id_configured()
            return self.async_create_entry(
                title=f"{entries[user_input[CONF_DREAME_ENTRY]]} Video",
                data={CONF_DREAME_ENTRY: user_input[CONF_DREAME_ENTRY]},
            )
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({vol.Required(CONF_DREAME_ENTRY): vol.In(entries)}),
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        return MovaVideoOptionsFlow()


class MovaVideoOptionsFlow(OptionsFlow):
    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        if user_input is not None:
            return self.async_create_entry(data=user_input)
        current = self.config_entry.options.get(CONF_HANDSHAKE, True)
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema({vol.Required(CONF_HANDSHAKE, default=current): bool}),
        )
