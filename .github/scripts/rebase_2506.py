from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise SystemExit(f"missing seam {label}: {old!r}")
    return text.replace(old, new, 1)


def update_density() -> None:
    path = Path("crates/pumpkin-world/src/generation/noise/router/chunk_density_function.rs")
    s = path.read_text()

    replacements = [
        ("fn get_buffer(len: usize) -> Box<[f32]> {", "fn get_buffer(len: usize) -> Vec<f32> {", "get_buffer type"),
        ("|| vec![0.0; len].into_boxed_slice(),", "|| vec![0.0; len],", "new buffer"),
        ("                buf.into_boxed_slice()\n", "                buf\n", "reused buffer"),
        ("fn recycle_buffer(buf: Box<[f32]>) {", "fn recycle_buffer(buf: Vec<f32>) {", "recycle type"),
        ("        pool.borrow_mut().push(Vec::from(buf));", "        pool.borrow_mut().push(buf);", "recycle push"),
        ("    pub(crate) start_buffer: Box<[f32]>,", "    pub(crate) start_buffer: Vec<f32>,", "start buffer"),
        ("    pub(crate) end_buffer: Box<[f32]>,", "    pub(crate) end_buffer: Vec<f32>,", "end buffer"),
        ("    pub(crate) const fn on_sampled_cell_corners(", "    pub(crate) fn on_sampled_cell_corners(", "sampled corners const"),
        (
            """        recycle_buffer(std::mem::replace(\n            &mut self.start_buffer,\n            Vec::new().into_boxed_slice(),\n        ));\n        recycle_buffer(std::mem::replace(\n            &mut self.end_buffer,\n            Vec::new().into_boxed_slice(),\n        ));""",
            """        recycle_buffer(std::mem::take(&mut self.start_buffer));\n        recycle_buffer(std::mem::take(&mut self.end_buffer));""",
            "interpolator drop",
        ),
        ("    pub(crate) cache: Box<[f32]>,", "    pub(crate) cache: Vec<f32>,", "flat cache"),
        (
            """        recycle_buffer(std::mem::replace(\n            &mut self.cache,\n            Vec::new().into_boxed_slice(),\n        ));""",
            "        recycle_buffer(std::mem::take(&mut self.cache));",
            "flat cache drop",
        ),
        ("    cache: Box<[f32]>,", "    cache: Vec<f32>,", "cache once"),
        ("            self.cache = vec![0.0; array.len()].into_boxed_slice();", "            self.cache = vec![0.0; array.len()];", "cache once resize"),
        ("            cache: Box::new([]),", "            cache: Vec::new(),", "cache once empty"),
        ("    pub(crate) cache: Box<[f32]>,", "    pub(crate) cache: Vec<f32>,", "cell cache"),
    ]
    for old, new, label in replacements:
        s = replace_once(s, old, new, label)

    needle = """impl CellCache {\n    #[must_use]\n    pub fn new("""
    replacement = """impl Drop for CellCache {\n    fn drop(&mut self) {\n        recycle_buffer(std::mem::take(&mut self.cache));\n    }\n}\n\nimpl CellCache {\n    #[must_use]\n    pub fn new("""
    s = replace_once(s, needle, replacement, "cell cache drop")
    path.write_text(s)


def update_proto_chunk() -> None:
    path = Path("crates/pumpkin-world/src/generation/proto_chunk.rs")
    s = path.read_text()

    setter_end = """        let index = self.local_pos_to_block_index(local_x, local_y, local_z);\n        self.flat_block_map[index] = block_state.id;\n    }\n\n    #[inline]\n    #[must_use]\n    pub fn get_biome"""
    direct = """        let index = self.local_pos_to_block_index(local_x, local_y, local_z);\n        self.flat_block_map[index] = block_state.id;\n    }\n\n    #[inline]\n    fn set_block_state_direct(\n        &mut self,\n        local_x: i32,\n        local_y: i32,\n        local_z: i32,\n        block_y: i16,\n        block_state: &BlockState,\n    ) {\n        if !block_state.is_air() {\n            let index = Self::local_position_to_height_map_index(local_x, local_z);\n            self.maybe_update_surface_height_map(index, block_y);\n            let block = BlockId::from_state_id(block_state.id);\n            let blocks_movement = blocks_movement(block_state, block);\n            if blocks_movement {\n                self.maybe_update_ocean_floor_height_map(index, block_y);\n            }\n            if blocks_movement || block_state.is_liquid() {\n                self.maybe_update_motion_blocking_height_map(index, block_y);\n                if !block.has_tag(tag::Block::MINECRAFT_LEAVES) {\n                    self.maybe_update_motion_blocking_no_leaves_height_map(index, block_y);\n                }\n            }\n        }\n\n        let index = self.local_pos_to_block_index(local_x, local_y, local_z);\n        self.flat_block_map[index] = block_state.id;\n    }\n\n    #[inline]\n    #[must_use]\n    pub fn get_biome"""
    s = replace_once(s, setter_end, direct, "direct setter insertion")

    prelude = """        let delta_y_step = 1.0 / v_count as f32;\n        let delta_x_z_step = 1.0 / h_count as f32;\n\n        noise_sampler.sample_start_density();"""
    prelude_new = """        let delta_y_step = 1.0 / v_count as f32;\n        let delta_x_z_step = 1.0 / h_count as f32;\n        let bottom_y = self.bottom_y() as i32;\n        let chunk_height = self.height() as i32;\n\n        noise_sampler.sample_start_density();"""
    s = replace_once(s, prelude, prelude_new, "populate prelude")

    s = replace_once(
        s,
        """            let sample_start_x = (self.start_cell_x(h_count) + cell_x) * h_count;\n            let block_x_base = self.start_block_x() + cell_x * h_count;""",
        """            let sample_start_x = (self.start_cell_x(h_count) + cell_x) * h_count;""",
        "block x base",
    )
    s = replace_once(
        s,
        """                let sample_start_z = (self.start_cell_z(h_count) + cell_z) * h_count;\n                let block_z_base = self.start_block_z() + cell_z * h_count;""",
        """                let sample_start_z = (self.start_cell_z(h_count) + cell_z) * h_count;""",
        "block z base",
    )

    loop_old = """                    for local_y in (0..v_count).rev() {\n                        let block_y = sample_start_y + local_y;\n                        noise_sampler.interpolate_y(local_y as f32 * delta_y_step);\n\n                        for local_x in 0..h_count {\n                            noise_sampler.interpolate_x(local_x as f32 * delta_x_z_step);\n                            let block_x = block_x_base + local_x;\n\n                            for local_z in 0..h_count {\n                                noise_sampler.interpolate_z(local_z as f32 * delta_x_z_step);\n                                let block_z = block_z_base + local_z;\n"""
    loop_new = """                    for local_y in (0..v_count).rev() {\n                        let block_y = sample_start_y + local_y;\n                        let chunk_local_y = block_y - bottom_y;\n                        if chunk_local_y < 0 || chunk_local_y >= chunk_height {\n                            continue;\n                        }\n                        noise_sampler.interpolate_y(local_y as f32 * delta_y_step);\n\n                        for local_x in 0..h_count {\n                            let chunk_local_x = cell_x * h_count + local_x;\n                            noise_sampler.interpolate_x(local_x as f32 * delta_x_z_step);\n\n                            for local_z in 0..h_count {\n                                let chunk_local_z = cell_z * h_count + local_z;\n                                noise_sampler.interpolate_z(local_z as f32 * delta_x_z_step);\n"""
    s = replace_once(s, loop_old, loop_new, "populate loop")

    s = replace_once(
        s,
        "                            self.set_block_state(block_x, block_y, block_z, block_state);",
        """                            self.set_block_state_direct(\n                                chunk_local_x,\n                                chunk_local_y,\n                                chunk_local_z,\n                                block_y as i16,\n                                block_state,\n                            );""",
        "direct setter call",
    )
    path.write_text(s)


update_density()
update_proto_chunk()
